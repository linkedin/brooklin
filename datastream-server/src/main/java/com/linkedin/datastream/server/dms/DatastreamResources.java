package com.linkedin.datastream.server.dms;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.ErrorLogger;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.security.AuthorizationException;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.ActionResult;
import com.linkedin.restli.server.BatchUpdateRequest;
import com.linkedin.restli.server.BatchUpdateResult;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.Finder;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.PathKeysParam;
import com.linkedin.restli.server.annotations.QueryParam;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.ResourceLevel;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.resources.CollectionResourceTemplate;


/*
 * Resources classes are used by rest.li to process corresponding http request.
 * Note that rest.li will instantiate an object each time it processes a request.
 * So do make it thread-safe when implementing the resources.
 */
@RestLiCollection(name = "datastream", keyName = DatastreamResources.KEY_NAME, namespace = "com.linkedin.datastream.server.dms")
public class DatastreamResources extends CollectionResourceTemplate<String, Datastream> {
  public static final String KEY_NAME = "datastreamId";
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamResources.class);
  private static final String CLASS_NAME = DatastreamResources.class.getSimpleName();

  private final DatastreamStore _store;
  private final Coordinator _coordinator;
  private final ErrorLogger _errorLogger;

  private static final String UPDATE_CALL = "updateCall";
  private static final String DELETE_CALL = "deleteCall";
  private static final String GET_CALL = "getCall";
  private static final String GET_ALL_CALL = "getAllCall";
  private static final String CREATE_CALL = "createCall";
  private static final String CALL_ERROR = "callError";
  private static final String FINDER_CALL = "finderCall";

  private static AtomicLong _createCallLatencyMs = new AtomicLong(0L);
  private static AtomicLong _deleteCallLatencyMs = new AtomicLong(0L);
  private static final Supplier<Long> CREATE_CALL_LATENCY_MS = () -> _createCallLatencyMs.get();
  private static final Supplier<Long> DELETE_CALL_LATENCY_MS = () -> _deleteCallLatencyMs.get();
  private static final String CREATE_CALL_LATENCY_MS_STRING = "createCallLatencyMs";
  private static final String DELETE_CALL_LATENCY_MS_STRING = "deleteCallLatencyMs";

  private final DynamicMetricsManager _dynamicMetricsManager;

  public DatastreamResources(DatastreamServer datastreamServer) {
    this(datastreamServer.getDatastreamStore(), datastreamServer.getCoordinator());
  }

  public DatastreamResources(DatastreamStore store, Coordinator coordinator) {
    _store = store;
    _coordinator = coordinator;
    _errorLogger = new ErrorLogger(LOG, _coordinator.getInstanceName());

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _dynamicMetricsManager.registerGauge(CLASS_NAME, CREATE_CALL_LATENCY_MS_STRING, CREATE_CALL_LATENCY_MS);
    _dynamicMetricsManager.registerGauge(CLASS_NAME, DELETE_CALL_LATENCY_MS_STRING, DELETE_CALL_LATENCY_MS);
  }

  /*
   * Update multiple datastreams. Throw exception if any of the updates is not valid:
   * 1. datastream doesn't exist
   * 2. status or destination is not present or gets modified
   * 3. more than one connector type in the batch update
   * 4. connector type doesn't support datastream updates or fails to validate the update
   */
  private void doUpdateDatastreams(Map<String, Datastream> datastreamMap) {
    LOG.info("Update datastream call with request: ", datastreamMap);
    _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, UPDATE_CALL, 1);
    if (datastreamMap.isEmpty()) {
      LOG.warn("Update datastream call with empty input.");
      return;
    }
    datastreamMap.forEach((key, datastream) -> {
      if (!key.equals(datastream.getName())) {
        _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            String.format("Failed to update %s because datastream name doesn't match. datastream: %s", key,
                datastream));
      }
      Datastream oldDatastream = _store.getDatastream(key);
      if (oldDatastream == null) {
        _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
            "Datastream to update does not exist: " + key);
      }

      // We support update datastreams for various use cases. But we don't support modifying the
      // destination or status (use pause/resume to update status). Writing into a different destination
      // should essentially be for a new datastream.
      if (!oldDatastream.hasDestination() || !datastream.hasDestination()) {
        _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            String.format("Failed to update %s because destination is not set. Are they initialized? old: %s, new: %s",
                key, oldDatastream, datastream));
      }
      if (!datastream.getDestination().equals(oldDatastream.getDestination())) {
        _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            String.format("Failed to update %s because destination is immutable. old: %s new: %s", key, oldDatastream,
                datastream));
      }

      if (!oldDatastream.hasStatus() || !datastream.hasStatus()) {
        _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            String.format("Failed to update %s because status is not present. Are they valid? old: %s, new: %s", key,
                oldDatastream, datastream));
      }
      if (!datastream.getStatus().equals(oldDatastream.getStatus())) {
        _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
            String.format("Failed to update %s. Can't update status in update request. old: %s new: %s", key,
                oldDatastream, datastream));
      }
    });

    try {
      _coordinator.validateDatastreamsUpdate(new ArrayList<>(datastreamMap.values()));
    } catch (DatastreamValidationException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Failed to validate datastream updates: ", e);
    }

    try {
      // Zookeeper has sequential consistency. So don't switch the order below: we need to make sure the datastreams
      // are updated before we touch the "assignments" node to avoid race condition
      for (String key : datastreamMap.keySet()) {
        _store.updateDatastream(key, datastreamMap.get(key), false);
      }
      _coordinator.broadcastDatastreamUpdate();
    } catch (DatastreamException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Could not complete datastreams update ", e);
    }
  }

  @Override
  public BatchUpdateResult<String, Datastream> batchUpdate(final BatchUpdateRequest<String, Datastream> entities) {
    doUpdateDatastreams(entities.getData());
    return new BatchUpdateResult<>(entities.getData()
        .keySet()
        .stream()
        .collect(Collectors.toMap(key -> key, key -> new UpdateResponse(HttpStatus.S_200_OK))));
  }

  @Override
  public UpdateResponse update(String key, Datastream datastream) {
    doUpdateDatastreams(Collections.singletonMap(key, datastream));
    return new UpdateResponse(HttpStatus.S_200_OK);
  }

  @Action(name = "pause", resourceLevel = ResourceLevel.ENTITY)
  public ActionResult<Void> pause(@PathKeysParam PathKeys pathKeys,
      @ActionParam("force") @Optional("false") boolean force) {
    String datastreamName = pathKeys.getAsString(KEY_NAME);
    Datastream datastream = _store.getDatastream(datastreamName);

    LOG.info("Received request to resume datastream {}", datastream);

    if (datastream == null) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
          "Datastream to pause does not exist: " + datastreamName);
    }

    if (!DatastreamStatus.READY.equals(datastream.getStatus())) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_405_METHOD_NOT_ALLOWED,
          "Can only pause a datastream in READY state: " + datastreamName);
    }

    List<Datastream> datastreamsToPause =
        force ? getGroupedDatastreams(datastream) : Collections.singletonList(datastream);
    LOG.info("Pausing datastreams {}", datastreamsToPause);
    for (Datastream d : datastreamsToPause) {
      try {
        if (DatastreamStatus.READY.equals(datastream.getStatus())) {
          d.setStatus(DatastreamStatus.PAUSED);
          _store.updateDatastream(d.getName(), d, true);
        } else {
          LOG.warn("Cannot pause datastream {}, as it is not in READY state. State: {}", d, datastream.getStatus());
        }
      } catch (DatastreamException e) {
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
            "Could not update datastream to paused state: " + d.getName(), e);
      }
    }

    LOG.info("Completed request for pausing datastream {}", datastream);

    return new ActionResult<>(HttpStatus.S_200_OK);
  }

  @Action(name = "resume", resourceLevel = ResourceLevel.ENTITY)
  public ActionResult<Void> resume(@PathKeysParam PathKeys pathKeys,
      @ActionParam("force") @Optional("false") boolean force) {
    String datastreamName = pathKeys.getAsString(KEY_NAME);
    Datastream datastream = _store.getDatastream(datastreamName);

    LOG.info("Received request to resume datastream {}", datastream);

    if (datastream == null) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
          "Datastream to resume does not exist: " + datastreamName);
    }

    if (!DatastreamStatus.PAUSED.equals(datastream.getStatus())) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_405_METHOD_NOT_ALLOWED,
          "Datastream is not paused, cannot resume: " + datastreamName);
    }

    List<Datastream> datastreamsToResume =
        force ? getGroupedDatastreams(datastream) : Collections.singletonList(datastream);
    LOG.info("Resuming datastreams {}", datastreamsToResume);
    for (Datastream d : datastreamsToResume) {
      try {
        if (DatastreamStatus.PAUSED.equals(datastream.getStatus())) {
          d.setStatus(DatastreamStatus.READY);
          _store.updateDatastream(d.getName(), d, true);
        } else {
          LOG.warn("Will not resume datastream {}, as it is already in paused state", d);
        }
      } catch (DatastreamException e) {
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
            "Could not update datastream to resume:  " + d.getName(), e);
      }
    }

    LOG.info("Completed request for resuming datastream {}", datastream);
    return new ActionResult<>(HttpStatus.S_200_OK);
  }

  /**
   * Given datastream and a map representing < source, list of partitions to pause >, pauses the partitions.
   * @param pathKeys
   * @param sourcePartitions StringMap of format <source, comma separated list of partitions or "*">. Example: <"FooTopic", "0,13,2">
   *                         or <"FooTopic","*">
   * @return
   */
  @Action(name = "pauseSourcePartitions", resourceLevel = ResourceLevel.ENTITY)
  public ActionResult<Void> pauseSourcePartitions(@PathKeysParam PathKeys pathKeys,
      @ActionParam("sourcePartitions") StringMap sourcePartitions) {

    // Get datastream.
    String datastreamName = pathKeys.getAsString(KEY_NAME);
    // Log for debugging purposes.
    LOG.info("pauseSourcePartitions called for datastream: {}, with partitions: {}", datastreamName, sourcePartitions);

    Datastream datastream = _store.getDatastream(datastreamName);
    if (datastream == null) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
          "Datastream does not exist: " + datastreamName);
    }

    pauseResumeSourcePartitionsPreCheck(datastream);

    // Convert the given json to actual map <source, partitions>
    // Note: These partitions will be added on the top of existing ones, it won't replace them.
    Map<String, Set<String>> newPausedSourcePartitionsMap =
        DatastreamUtils.parseSourcePartitionsStringMap(sourcePartitions);

    // Get the existing set of paused partitions from datastream object.
    // Convert the existing json to map <source, partitions>
    Map<String, Set<String>> existingPausedSourcePartitionsMap =
        DatastreamUtils.getDatastreamSourcePartitions(datastream);

    // Now add the given set of paused partitions to existing set of paused partitions.
    for (String source : newPausedSourcePartitionsMap.keySet()) {
      Set<String> newPartitions = newPausedSourcePartitionsMap.get(source);

      // If the source doesn't exist already, add it.
      if (!existingPausedSourcePartitionsMap.containsKey(source)) {
        existingPausedSourcePartitionsMap.put(source, new HashSet<>());
      }

      // Get existing set of paused partitions for that source
      Set<String> existingPausedPartitions = existingPausedSourcePartitionsMap.get(source);
      existingPausedPartitions.addAll(newPartitions);
    }

    // Now convert the Map to Json and update datastream object.
    String newPausedPartitionsJson = "";
    if (!existingPausedSourcePartitionsMap.isEmpty()) {
      newPausedPartitionsJson = JsonUtils.toJson(existingPausedSourcePartitionsMap);
    }
    datastream.getMetadata().put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, newPausedPartitionsJson);

    // Now validate the operation
    // Note: This is connector specific logic (for example: Kafka mirror maker will convert any "*" into actual
    // list of partitions).
    //TODO: Will need to add code in non-MM connectors to invalidate the operation.
    try {
      _coordinator.validateDatastreamsUpdate(Collections.singletonList(datastream));
    } catch (DatastreamValidationException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Failed to validate datastream updates: ", e);
    }

    // Persist in zk.
    try {
      _store.updateDatastream(datastream.getName(), datastream, true);
      _coordinator.broadcastDatastreamUpdate();
    } catch (Exception e) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Could not update datastream's paused partitions: " + datastream.getName(), e);
    }

    LOG.info("Completed request to pause datastream: {}, source partitions: {}", datastreamName, sourcePartitions);
    return new ActionResult<>(HttpStatus.S_200_OK);
  }

  /**
   * Given datastream and a map representing < source, list of partitions to resume >, resumes the partitions.
   * @param pathKeys
   * @param sourcePartitions StringMap of format <source, comma separated list of partitions or "*">. Example: <"FooTopic", "0,13,2">
   *                         or <"FooTopic","*">
   * @return
   */
  @Action(name = "resumeSourcePartitions", resourceLevel = ResourceLevel.ENTITY)
  public ActionResult<Void> resumeSourcePartitions(@PathKeysParam PathKeys pathKeys,
      @ActionParam("sourcePartitions") StringMap sourcePartitions) {

    // Get datastream.
    String datastreamName = pathKeys.getAsString(KEY_NAME);
    // Log for debugging purposes.
    LOG.info("resumeSourcePartitions called for datastream: {}, with partitions: {}", datastreamName,
        sourcePartitions);

    Datastream datastream = _store.getDatastream(datastreamName);
    if (datastream == null) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
          "Datastream does not exist: " + datastreamName);
    }

    pauseResumeSourcePartitionsPreCheck(datastream);

    // Convert the given json to actual map <source, partitions>
    // Note: These partitions will be added on the top of existing ones, it won't replace them.
    Map<String, Set<String>> sourcePartitionsToResumeMap =
        DatastreamUtils.parseSourcePartitionsStringMap(sourcePartitions);

    // Get the existing set of paused partitions from datastream object.
    // Convert the existing json to map <source, partitions>
    Map<String, Set<String>> existingPausedSourcePartitionsMap =
        DatastreamUtils.getDatastreamSourcePartitions(datastream);

    if (existingPausedSourcePartitionsMap.size() == 0) {
      return new ActionResult<>(HttpStatus.S_200_OK);
    }

    // Now go through partitions to resume.
    for (String source : sourcePartitionsToResumeMap.keySet()) {
      Set<String> partitionsToResume = sourcePartitionsToResumeMap.get(source);

      // If there is nothing paused for given source, no need to do anything for that source.
      if (existingPausedSourcePartitionsMap.containsKey(source)) {
        // Get existing set of paused partitions for that source
        Set<String> existingPausedPartitions = existingPausedSourcePartitionsMap.get(source);

        // In case we are supposed to resume all partitions, remove the source.
        // In that case, we ignore other partitions that were mentioned in the resume list for that source.
        if (partitionsToResume.contains(DatastreamMetadataConstants.REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC)) {
          existingPausedSourcePartitionsMap.remove(source);
        } else {
          existingPausedPartitions.removeAll(partitionsToResume);
          // If no partition left, remove the source.
          if (existingPausedPartitions.size() == 0) {
            existingPausedSourcePartitionsMap.remove(source);
          }
        }
      }
    }

    // Now convert the Map to Json and update datastream object.
    String newPausedSourcePartitionsJson = "";
    if (!existingPausedSourcePartitionsMap.isEmpty()) {
      newPausedSourcePartitionsJson = JsonUtils.toJson(existingPausedSourcePartitionsMap);
    }
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, newPausedSourcePartitionsJson);

    // Validate changes
    try {
      _coordinator.validateDatastreamsUpdate(Collections.singletonList(datastream));
    } catch (DatastreamValidationException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Failed to validate datastream updates: ", e);
    }

    // Persist in zk.
    try {
      _store.updateDatastream(datastream.getName(), datastream, true);
      _coordinator.broadcastDatastreamUpdate();
    } catch (DatastreamException e) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Could not update datastream's paused partitions: " + datastream.getName(), e);
    }

    LOG.info("Completed request to resume datastream: {}, source partitions: {}", datastreamName, sourcePartitions);
    return new ActionResult<>(HttpStatus.S_200_OK);
  }

  @Override
  public UpdateResponse delete(String datastreamName) {
    if (null == _store.getDatastream(datastreamName)) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
          "Datastream requested to be deleted does not exist: " + datastreamName);
    }

    try {
      LOG.info("Delete datastream called for datastream " + datastreamName);

      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, DELETE_CALL, 1);
      Instant startTime = Instant.now();
      _store.deleteDatastream(datastreamName);
      _deleteCallLatencyMs.set(Duration.between(startTime, Instant.now()).toMillis());

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Delete failed for datastream: " + datastreamName, e);
    }

    return null;
  }

  // Returning null will automatically trigger a 404 Not Found response
  @Override
  public Datastream get(String name) {
    try {
      LOG.info("Get datastream called for datastream {}", name);
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, GET_CALL, 1);
      return _store.getDatastream(name);
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Get datastream failed for datastream: " + name, e);
    }

    return null;
  }

  @SuppressWarnings("deprecated")
  @Override
  public List<Datastream> getAll(@Context PagingContext pagingContext) {
    try {
      LOG.info("Get all datastreams called with paging context {}", pagingContext);
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, GET_ALL_CALL, 1);
      List<Datastream> ret = RestliUtils.withPaging(_store.getAllDatastreams(), pagingContext)
          .map(_store::getDatastream)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
      LOG.debug("Result collected for getAll {}", ret);
      return ret;
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Get all datastreams failed.", e);
    }

    return Collections.emptyList();
  }

  /**
   * You can access this FINDER method via /resources/datastream?q=findDuplicates&datastreamName=name
   */
  @SuppressWarnings("deprecated")
  @Finder("findGroup")
  public List<Datastream> findGroup(@Context PagingContext pagingContext,
      @QueryParam("datastreamName") String datastreamName) {
    try {
      LOG.info("findDuplicates called with paging context {}", pagingContext);
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, FINDER_CALL, 1);
      Datastream datastream = _store.getDatastream(datastreamName);
      if (datastream == null) {
        _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_404_NOT_FOUND,
            "Datastream not found: " + datastreamName);
      }
      List<Datastream> ret = RestliUtils.withPaging(getGroupedDatastreams(datastream).stream(), pagingContext)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
      LOG.debug("Result collected for findDuplicates: {}", ret);
      return ret;
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Call findDuplicates failed.", e);
    }
    return Collections.emptyList();
  }

  @Override
  public CreateResponse create(Datastream datastream) {
    try {
      LOG.info("Create datastream called with datastream {}", datastream);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Handling request on object: {} thread: {}", this, Thread.currentThread());
      }

      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CREATE_CALL, 1);

      // rest.li has done this mandatory field check in the latest version.
      // Just in case we roll back to an earlier version, let's do the validation here anyway
      DatastreamUtils.validateNewDatastream(datastream);
      Validate.isTrue(datastream.hasName(), "Must specify name of Datastream!");
      Validate.isTrue(datastream.hasConnectorName(), "Must specify connectorType!");
      Validate.isTrue(datastream.hasSource(), "Must specify source of Datastream!");
      Validate.isTrue(datastream.hasMetadata(), "Missing metadata for Datastream!");

      StringMap metadataMap = datastream.getMetadata();
      Validate.isTrue(metadataMap.containsKey(DatastreamMetadataConstants.OWNER_KEY),
          "Must specify owner of Datastream");

      if (datastream.hasDestination() && datastream.getDestination().hasConnectionString()) {
        metadataMap.put(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY, "true");
      }

      Instant startTime = Instant.now();

      LOG.debug("Sanity check is finished, initializing datastream");

      // Before the initializeDatastream (which could be heavy depends on the types of datastreams),
      // quickly check whether the datastream has already existed.
      if (_store.getDatastream(datastream.getName()) != null) {
        throw new DatastreamAlreadyExistsException();
      }

      _coordinator.initializeDatastream(datastream);

      LOG.debug("Persisting initialized datastream to zookeeper: {}", datastream);

      _store.createDatastream(datastream.getName(), datastream);

      Duration delta = Duration.between(startTime, Instant.now());
      _createCallLatencyMs.set(delta.toMillis());

      LOG.info("Datastream persisted to zookeeper, total time used: {} ms", delta.toMillis());
      return new CreateResponse(datastream.getName(), HttpStatus.S_201_CREATED);
    } catch (IllegalArgumentException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST,
          "Invalid input params for create request", e);
    } catch (DatastreamValidationException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_400_BAD_REQUEST, "Failed to initialize Datastream: ",
          e);
    } catch (DatastreamAlreadyExistsException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_409_CONFLICT,
          "Datastream with the same name already exists: " + datastream, e);
    } catch (AuthorizationException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_401_UNAUTHORIZED,
          "Datastream creation denied due to insufficient authorization: " + datastream, e);
    } catch (Exception e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
          "Unexpected error during datastream creation: " + datastream, e);
    }

    // Should never get here because we throw on any errors
    return null;
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();

    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, UPDATE_CALL)));
    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, DELETE_CALL)));
    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, GET_CALL)));
    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, GET_ALL_CALL)));
    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, CREATE_CALL)));
    metrics.add(new BrooklinMeterInfo(MetricRegistry.name(CLASS_NAME, CALL_ERROR)));

    metrics.add(new BrooklinGaugeInfo(MetricRegistry.name(CLASS_NAME, CREATE_CALL_LATENCY_MS_STRING)));
    metrics.add(new BrooklinGaugeInfo(MetricRegistry.name(CLASS_NAME, DELETE_CALL_LATENCY_MS_STRING)));

    return Collections.unmodifiableList(metrics);
  }

  private List<Datastream> getGroupedDatastreams(Datastream datastream) {
    String taskPrefix = DatastreamUtils.getTaskPrefix(datastream);
    if (StringUtils.isEmpty(taskPrefix)) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_412_PRECONDITION_FAILED,
          "Datastream does not have Task Prefix: " + datastream.getName());
    }
    return _store.getAllDatastreams()
        .map(_store::getDatastream)
        .filter(d -> taskPrefix.equals(DatastreamUtils.getTaskPrefix(d)))
        .collect(Collectors.toList());
  }

  private void pauseResumeSourcePartitionsPreCheck(Datastream datastream) {
    // Make sure it is in ready state.
    if (!DatastreamStatus.READY.equals(datastream.getStatus())) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_405_METHOD_NOT_ALLOWED,
          "Can only pause/resume partitions for a datastream in READY state: " + datastream.getName());
    }

    // Note: Pausing datastreams goes with an assumption that they are not a part of any datastream group
    // Need to change this logic to update all datastreams in case that assumption changes.
    if (getGroupedDatastreams(datastream).size() > 1) {
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_405_METHOD_NOT_ALLOWED,
          "Can only pause/resume partitions for a datastream that are not a part of any datastremgroup : "
              + datastream.getName());
    }

    // Make sure the operation is supported for the given datastream:
    try {
      _coordinator.isDatastreamUpdateTypeSupported(datastream,
          DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    } catch (DatastreamValidationException e) {
      _dynamicMetricsManager.createOrUpdateMeter(CLASS_NAME, CALL_ERROR, 1);
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_405_METHOD_NOT_ALLOWED,
          "Pause/resume operation is not supported for datastream : " + datastream.getName());
    }
  }
}
