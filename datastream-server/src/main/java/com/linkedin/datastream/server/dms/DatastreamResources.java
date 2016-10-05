package com.linkedin.datastream.server.dms;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.datastream.metrics.BrooklinMetric;
import com.linkedin.datastream.metrics.StaticBrooklinMetric;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.UpdateResponse;
import com.linkedin.restli.server.annotations.Context;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTemplate;


/*
 * Resources classes are used by rest.li to process corresponding http request.
 * Note that rest.li will instantiate an object each time it processes a request.
 * So do make it thread-safe when implementing the resources.
 */
@RestLiCollection(name = "datastream", namespace = "com.linkedin.datastream.server.dms")
public class DatastreamResources extends CollectionResourceTemplate<String, Datastream> {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamResources.class);
  private static final String CLASS_NAME = DatastreamResources.class.getSimpleName();

  private final DatastreamStore _store;
  private final Coordinator _coordinator;
  private final ErrorLogger _errorLogger;

  private static final Meter UPDATE_CALL = new Meter();
  private static final Meter DELETE_CALL = new Meter();
  private static final Meter GET_CALL = new Meter();
  private static final Meter GET_ALL_CALL = new Meter();
  private static final Meter CREATE_CALL = new Meter();
  private static final Meter CALL_ERROR = new Meter();

  private static AtomicLong _createCallLatencyMs = new AtomicLong(0L);
  private static AtomicLong _deleteCallLatencyMs = new AtomicLong(0L);
  private static final Gauge<Long> CREATE_CALL_LATENCY_MS = () -> _createCallLatencyMs.get();
  private static final Gauge<Long> DELETE_CALL_LATENCY_MS = () -> _deleteCallLatencyMs.get();

  public DatastreamResources(DatastreamServer datastreamServer) {
    _store = datastreamServer.getDatastreamStore();
    _coordinator = datastreamServer.getCoordinator();
    _errorLogger = new ErrorLogger(LOG);
  }

  @Override
  public UpdateResponse update(String key, Datastream datastream) {
    UPDATE_CALL.mark();
    // TODO: behavior of updating a datastream is not fully defined yet; block this method for now
    return new UpdateResponse(HttpStatus.S_405_METHOD_NOT_ALLOWED);
  }

  @Override
  public UpdateResponse delete(String key) {
    try {
      LOG.info("Delete datastream called for datastream " + key);
      DELETE_CALL.mark();
      Instant startTime = Instant.now();
      _store.deleteDatastream(key);
      _deleteCallLatencyMs.set(Duration.between(startTime, Instant.now()).toMillis());

      return new UpdateResponse(HttpStatus.S_200_OK);
    } catch (Exception e) {
      CALL_ERROR.mark();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
        "delete datastream failed for datastream: " + key, e);
    }

    return null;
  }

  // Returning null will automatically trigger a 404 Not Found response
  @Override
  public Datastream get(String name) {
    try {
      LOG.info(String.format("Get datastream called for datastream %s", name));
      GET_CALL.mark();
      return _store.getDatastream(name);
    } catch (Exception e) {
      CALL_ERROR.mark();
      _errorLogger.logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR,
        "Get datastream failed for datastream: " + name, e);
    }

    return null;
  }

  @SuppressWarnings("deprecated")
  @Override
  public List<Datastream> getAll(@Context PagingContext pagingContext) {
    try {
      LOG.info(String.format("Get all datastreams called with paging context %s", pagingContext));
      GET_ALL_CALL.mark();
      List<Datastream> ret = RestliUtils.withPaging(_store.getAllDatastreams(), pagingContext).map(_store::getDatastream)
        .filter(stream -> stream != null).collect(Collectors.toList());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Result collected for getAll: " + ret);
      }
      return ret;
    } catch (Exception e) {
      CALL_ERROR.mark();
      _errorLogger
        .logAndThrowRestLiServiceException(HttpStatus.S_500_INTERNAL_SERVER_ERROR, "Get all datastreams failed.", e);
    }

    return Collections.emptyList();
  }

  @Override
  public CreateResponse create(Datastream datastream) {
    try {
      LOG.info(String.format("Create datastream called with datastream %s", datastream));
      if (LOG.isDebugEnabled()) {
        LOG.debug("Handling request on object: " + this.toString() + " thread: " + Thread.currentThread());
      }
      CREATE_CALL.mark();

      // rest.li has done this mandatory field check in the latest version.
      // Just in case we roll back to an earlier version, let's do the validation here anyway
      if (!datastream.hasName()) {
        CALL_ERROR.mark();
        return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Must specify name of Datastream!");
      }
      if (!datastream.hasConnectorName()) {
        CALL_ERROR.mark();
        return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Must specify connectorType!");
      }
      if (!datastream.hasSource()) {
        CALL_ERROR.mark();
        return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Must specify source of Datastream!");
      }

      if (!datastream.hasMetadata()) {
        datastream.setMetadata(new StringMap());
      }

      if (!datastream.getMetadata().containsKey(DatastreamMetadataConstants.OWNER_KEY)) {
        CALL_ERROR.mark();
        return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Must specify owner of Datastream!");
      }

      if (datastream.hasDestination() && datastream.getDestination().hasConnectionString()) {
        datastream.getMetadata().put(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY, "true");
      }

      Instant startTime = Instant.now();

      LOG.debug("Done with preliminary check on the datastream. About to initialize it.");
      try {
        _coordinator.initializeDatastream(datastream);
      } catch (DatastreamValidationException e) {
        CALL_ERROR.mark();
        return _errorLogger.logAndGetResponse(HttpStatus.S_400_BAD_REQUEST, "Failed to initialize Datastream: ", e);
      }

      LOG.debug("Successully initialize datastream to: " + datastream);
      LOG.debug("Persist the datastream to zookeeper.");
      try {
        _store.createDatastream(datastream.getName(), datastream);
      } catch (DatastreamAlreadyExistsException e) {
        CALL_ERROR.mark();
        return _errorLogger
          .logAndGetResponse(HttpStatus.S_409_CONFLICT, "Failed to create datastream: " + datastream, e);
      }

      Duration delta = Duration.between(startTime, Instant.now());
      _createCallLatencyMs.set(delta.toMillis());

      LOG.debug(
        "Datastream persisted to zookeeper. Time taken for initialization and persistence: " + delta.toMillis() + "ms");
      return new CreateResponse(datastream.getName(), HttpStatus.S_201_CREATED);
    } catch (Exception e) {
      CALL_ERROR.mark();
      return _errorLogger
        .logAndGetResponse(HttpStatus.S_500_INTERNAL_SERVER_ERROR, "Failed to create datastream: " + datastream, e);
    }
  }

  public static List<BrooklinMetric> getMetrics() {
    List<BrooklinMetric> metrics = new ArrayList<>();

    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "updateCall"), UPDATE_CALL));
    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "deleteCall"), DELETE_CALL));
    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "getCall"), GET_CALL));
    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "getAllCall"), GET_ALL_CALL));
    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "createCall"), CREATE_CALL));
    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "callError"), CALL_ERROR));

    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "createCallLatencyMs"), CREATE_CALL_LATENCY_MS));
    metrics.add(new StaticBrooklinMetric(MetricRegistry.name(CLASS_NAME, "deleteCallLatencyMs"), DELETE_CALL_LATENCY_MS));

    return Collections.unmodifiableList(metrics);
  }
}
