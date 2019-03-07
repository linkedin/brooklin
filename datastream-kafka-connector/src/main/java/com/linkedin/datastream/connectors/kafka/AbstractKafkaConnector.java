/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.DiagnosticsAware;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.diag.DatastreamPositionResponse;
import com.linkedin.datastream.common.diag.PhysicalSourcePosition;
import com.linkedin.datastream.common.diag.PhysicalSources;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * Base class for connectors that consume from Kafka.
 */
public abstract class AbstractKafkaConnector implements Connector, DiagnosticsAware {

  public static final String IS_GROUP_ID_HASHING_ENABLED = "isGroupIdHashingEnabled";

  private static final Duration CANCEL_TASK_TIMEOUT = Duration.ofSeconds(30);
  private static final String TOPIC_KEY = "topic";
  private static final String OFFSETS_KEY = "offsets";

  protected final String _connectorName;
  protected final KafkaBasedConnectorConfig _config;
  protected final GroupIdConstructor _groupIdConstructor;
  protected final String _clusterName;
  protected final ConcurrentHashMap<DatastreamTask, AbstractKafkaBasedConnectorTask> _runningTasks =
      new ConcurrentHashMap<>();

  private final Logger _logger;
  private final AtomicInteger threadCounter = new AtomicInteger(0);
  private final ConcurrentHashMap<DatastreamTask, Thread> _taskThreads = new ConcurrentHashMap<>();

  // A daemon executor to constantly check whether all tasks are running and restart them if not.
  private ScheduledExecutorService _daemonThreadExecutorService =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(@NotNull Runnable r) {
          Thread t = new Thread(r);
          t.setDaemon(true);
          t.setName(String.format("%s daemon thread", _connectorName));
          return t;
        }
      });

  enum DiagnosticsRequestType {
    DATASTREAM_STATE,
    POSITION
  }

  public AbstractKafkaConnector(String connectorName, Properties config, GroupIdConstructor groupIdConstructor,
      String clusterName, Logger logger) {
    _connectorName = connectorName;
    _logger = logger;
    _clusterName = clusterName;
    _config = new KafkaBasedConnectorConfig(config);
    _groupIdConstructor = groupIdConstructor;
  }

  protected abstract AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task);

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {
    _logger.info("onAssignmentChange called with tasks {}", tasks);

    HashSet<DatastreamTask> toCancel = new HashSet<>(_runningTasks.keySet());
    toCancel.removeAll(tasks);

    for (DatastreamTask task : toCancel) {
      AbstractKafkaBasedConnectorTask connectorTask = _runningTasks.remove(task);
      connectorTask.stop();
      _taskThreads.remove(task);
    }

    for (DatastreamTask task : tasks) {
      AbstractKafkaBasedConnectorTask kafkaBasedConnectorTask = _runningTasks.get(task);
      if (kafkaBasedConnectorTask != null) {
        kafkaBasedConnectorTask.checkForUpdateTask(task);
        // make sure to replace the DatastreamTask with most up to date info
        _runningTasks.put(task, kafkaBasedConnectorTask);
        continue; // already running
      }

      createKafkaConnectorTask(task);
    }
  }

  public Thread createTaskThread(AbstractKafkaBasedConnectorTask task) {
    Thread t = new Thread(task);
    t.setDaemon(true);
    t.setName(String.format("%s task thread %s %d", _connectorName, task.getTaskName(), threadCounter.incrementAndGet()));
    t.setUncaughtExceptionHandler((thread, e) -> {
      _logger.error(String.format("thread %s has died due to uncaught exception.", thread.getName()), e);
    });
    return t;
  }

  private void createKafkaConnectorTask(DatastreamTask task) {
    _logger.info("creating task for {}.", task);
    AbstractKafkaBasedConnectorTask connectorTask = createKafkaBasedConnectorTask(task);
    _runningTasks.put(task, connectorTask);
    Thread taskThread = createTaskThread(connectorTask);
    _taskThreads.put(task, taskThread);
    taskThread.start();
  }

  @Override
  public void start(CheckpointProvider checkpointProvider) {
    _daemonThreadExecutorService.scheduleAtFixedRate(() -> {
      try {
        if (!_runningTasks.isEmpty()) {
          _logger.info("Checking status of running kafka connector tasks.");
          _runningTasks.keySet().forEach(this::restartIfNotRunning);
        } else {
          _logger.warn("connector received no datastreams tasks yet.");
        }
      } catch (Exception e) {
        // catch any exceptions here so that subsequent check can continue
        // see java doc of scheduleAtFixedRate
        _logger.warn("Failed to check status of kafka connector tasks.", e);
      }
    }, _config.getDaemonThreadIntervalSeconds(), _config.getDaemonThreadIntervalSeconds(), TimeUnit.SECONDS);
  }

  /**
   * If the {@link AbstractKafkaBasedConnectorTask} corresponding to the {@link DatastreamTask} is not running, Restart it.
   * @param task Datastream task which needs to checked and restarted if it is not running.
   */
  protected void restartIfNotRunning(DatastreamTask task) {
    if (!isTaskRunning(task)) {
      _logger.warn("Detected that the kafka connector task is not running for datastream task {}. Restarting it", task);
      boolean stopped = stopTask(task);
      if (stopped) {
        createKafkaConnectorTask(task);
      } else {
        _logger.error("Datastream task {} could not be stopped.");
      }
    }
  }

  /**
   * Stop the datastream task and wait for it to stop. If it has not stopped within a timeout, interrupt the thread.
   */
  private boolean stopTask(DatastreamTask datastreamTask) {
    try {
      AbstractKafkaBasedConnectorTask kafkaTask = _runningTasks.get(datastreamTask);
      kafkaTask.stop();
      boolean stopped = kafkaTask.awaitStop(CANCEL_TASK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      if (!stopped) {
        _logger.warn("Task {} took longer than {} ms to stop. Interrupting the thread.", datastreamTask,
            CANCEL_TASK_TIMEOUT.toMillis());
        _taskThreads.get(datastreamTask).interrupt();
      }
      _runningTasks.remove(datastreamTask);
      _taskThreads.remove(datastreamTask);
      return true;
    } catch (InterruptedException e) {
      _logger.warn("Caught exception while trying to stop the datastream task {}", datastreamTask, e);
    }

    return false;
  }

  /**
   * Check if the {@link AbstractKafkaBasedConnectorTask} corresponding to the {@link DatastreamTask} is running.
   * @param datastreamTask Datastream task that needs to be checked whether it is running.
   * @return true if it is running, false if it is not.
   */
  protected boolean isTaskRunning(DatastreamTask datastreamTask) {
    Thread taskThread = _taskThreads.get(datastreamTask);
    AbstractKafkaBasedConnectorTask kafkaTask = _runningTasks.get(datastreamTask);
    return (taskThread != null && taskThread.isAlive()
        && (System.currentTimeMillis() - kafkaTask.getLastPolledTimeMs()) < _config.getNonGoodStateThresholdMs());
  }

  @Override
  public void stop() {
    _daemonThreadExecutorService.shutdown();
    // Try to stop the the tasks
    _runningTasks.keySet().forEach(datastreamTask -> stopTask(datastreamTask));
    _runningTasks.clear();
    _taskThreads.clear();
    _logger.info("Connector stopped.");
  }

  @Override
  public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    // validate for paused partitions
    validatePausedPartitions(datastreams, allDatastreams);
  }

  private void validatePausedPartitions(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    for (Datastream ds : datastreams) {
      validatePausedPartitions(ds, allDatastreams);
    }
  }

  private void validatePausedPartitions(Datastream datastream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    Map<String, Set<String>> pausedSourcePartitionsMap = DatastreamUtils.getDatastreamSourcePartitions(datastream);

    for (String source : pausedSourcePartitionsMap.keySet()) {
      Set<String> newPartitions = pausedSourcePartitionsMap.get(source);

      // Validate that partitions actually exist and convert any "*" to actual list of partitions.
      // For that, get the list of existing partitions first.
      List<PartitionInfo> partitionInfos = getKafkaTopicPartitions(datastream, source);
      Set<String> allPartitions = new HashSet<>();
      for (PartitionInfo info : partitionInfos) {
        allPartitions.add(String.valueOf(info.partition()));
      }

      // if there is any * in the new list, just convert it to actual list of partitions.
      if (newPartitions.contains(DatastreamMetadataConstants.REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC)) {
        newPartitions.clear();
        newPartitions.addAll(allPartitions);
      } else {
        // Else make sure there aren't any partitions that don't exist.
        newPartitions.retainAll(allPartitions);
      }
    }

    // Now write back the set to datastream
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedSourcePartitionsMap));
  }

  private List<PartitionInfo> getKafkaTopicPartitions(Datastream datastream, String topic)
      throws DatastreamValidationException {
    List<PartitionInfo> partitionInfos;

    DatastreamSource source = datastream.getSource();
    String connectionString = source.getConnectionString();

    KafkaConnectionString parsed = KafkaConnectionString.valueOf(connectionString);

    try (Consumer<?, ?> consumer = KafkaConnectorTask.createConsumer(_config.getConsumerFactory(),
        _config.getConsumerProps(), "KafkaConnectorPartitionFinder", parsed)) {
      partitionInfos = consumer.partitionsFor(topic);
      if (partitionInfos == null) {
        throw new DatastreamValidationException("Can't get partition info from kafka for topic: " + topic);
      }
    } catch (Exception e) {
      throw new DatastreamValidationException(
          "Exception received while retrieving info on kafka topic partitions: " + e);
    }

    return partitionInfos;
  }

  /**
   * Process requests made to the ServerComponentHealthResources diagnostics endpoint. Currently able to process
   * requests for datastream_state, for which it will return sets of auto and manually paused topic partitions.
   * Sample query: /datastream_state?datastream=PizzaDatastream
   * Sample response: {"datastream":"testProcessDatastreamStates",
   *      "autoPausedPartitions":{"SaltyPizza-6":{"reason":"SEND_ERROR"},"SaltyPizza-17":{"reason":"SEND_ERROR"}},
   *      "manualPausedPartitions":{"YummyPizza":["19"],"SaltyPizza":["1","9","25"]}}
   * @param query the query
   * @return JSON string result, or null if the query could not be understood
   */
  @Override
  public String process(String query) {
    _logger.info("Processing query: {}", query);
    try {
      URI uri = new URI(query);
      String path = getPath(query, _logger);
      if (path != null && path.equalsIgnoreCase(DiagnosticsRequestType.POSITION.toString())) {
        return processDatastreamPositionRequest(uri);
      } else if (path != null && path.equalsIgnoreCase(DiagnosticsRequestType.DATASTREAM_STATE.toString())) {
        String response = processDatastreamStateRequest(uri);
        _logger.info("Query: {} returns response: {}", query, response);
        return response;
      } else {
        _logger.error("Could not process query {} with path {}", query, path);
      }
    } catch (Exception e) {
      _logger.error("Failed to process query {}", query, e);
    }
    return null;
  }

  private String processDatastreamStateRequest(URI request) {
    _logger.info("process Datastream state request: {}", request);
    Optional<String> datastreamName = extractQueryParam(request, DATASTREAM_KEY);
    return datastreamName.map(streamName -> _runningTasks.values()
        .stream()
        .filter(task -> task.hasDatastream(streamName))
        .findFirst()
        .map(AbstractKafkaBasedConnectorTask::getKafkaDatastreamStatesResponse)
        .orElse(null)).map(KafkaDatastreamStatesResponse::toJson).orElse(null);
  }

  /**
   * Processes a datastream position request.
   * @param request the request parameters
   * @return a datastream position request, serialized in JSON
   * @see com.linkedin.datastream.common.diag.DatastreamPositionResponse for the contents of such a response
   * @see AbstractKafkaBasedConnectorTask#getPositionResponse() for how the response is compiled
   */
  private String processDatastreamPositionRequest(URI request) {
    _logger.debug("Processing datastream position request: {}", request);

    // Determine if the user is asking specifically for the offset position data
    boolean offsetPositionTypeRequested = extractQueryParam(request, OFFSETS_KEY).map(Boolean::parseBoolean).orElse(false);

    DatastreamPositionResponse response = _runningTasks.values()
        .stream()
        .map(task -> offsetPositionTypeRequested ? task.getOffsetPositionResponse() : task.getPositionResponse())
        .reduce(DatastreamPositionResponse::merge)
        .orElse(new DatastreamPositionResponse());
    _logger.debug("Unfiltered datastream position response: {}", response);

    // Filter datastreams if specified -- may be needed if output is too large
    extractQueryParam(request, DATASTREAM_KEY).ifPresent(name -> response.retainAll(Collections.singleton(name)));

    // Filter topic if specified -- may be needed if output is too large
    extractQueryParam(request, TOPIC_KEY).ifPresent(topic -> {
      for (PhysicalSources sources : response.getDatastreamToPhysicalSources().values()) {
        Map<String, PhysicalSourcePosition> positions = new HashMap<>(sources.getPhysicalSourceToPosition());
        for (String position : positions.keySet()) {
          if (!position.matches("^" + topic + "-\\d+$")) {
            positions.remove(position);
          }
        }
      }
    });
    _logger.debug("Filtered datastream position response: {}", response);

    return DatastreamPositionResponse.toJson(response);
  }

  /**
   * Aggregates the responses from all the instances into a single JSON response.
   * Sample query: /datastream_state?datastream=PizzaDatastream
   * Sample response:
   * {"instance2": "{"datastream":"testProcessDatastreamStates",
   *      "autoPausedPartitions":{"SaltyPizza-6":{"reason":"SEND_ERROR"},"SaltyPizza-17":{"reason":"SEND_ERROR"}},
   *      "manualPausedPartitions":{"YummyPizza":["19"],"SaltyPizza":["1","9","25"]}}",
   * "instance1": "{"datastream":"testProcessDatastreamStates",
   *      "autoPausedPartitions":{"YummyPizza-0":{"reason":"SEND_ERROR"},"YummyPizza-10":{"reason":"SEND_ERROR"}},
   *      "manualPausedPartitions":{"YummyPizza":["11","23","4"],"SaltyPizza":["77","2","5"]}}"
   * }
   * @param query the query
   * @param responses a map of hosts to their responses
   * @return a JSON string which is aggregated from all the responses, or null if the query could not be understood
   */
  @Override
  public String reduce(String query, Map<String, String> responses) {
    _logger.info("Reducing query {} with responses from {}.", query, responses.keySet());
    try {
      String path = getPath(query, _logger);
      if (path != null && path.equalsIgnoreCase(DiagnosticsRequestType.POSITION.toString())) {
        return reduceDatastreamPositionResponses(responses);
      } else if (path != null && path.equalsIgnoreCase(DiagnosticsRequestType.DATASTREAM_STATE.toString())) {
        return JsonUtils.toJson(responses);
      }
    } catch (Exception e) {
      _logger.error(String.format("Failed to reduce responses %s", query), e);
      return null;
    }
    return null;
  }

  /**
   * Aggregates all of the position responses from all the instances into a single JSON response.
   * @param responses the individual responses
   * @return an aggregated response
   * @see com.linkedin.datastream.common.diag.DatastreamPositionResponse for the contents of such a response
   */
  private String reduceDatastreamPositionResponses(Map<String, String> responses) {
    List<DatastreamPositionResponse> responseList = new ArrayList<>();
    responses.forEach((instance, json) -> {
      try {
        DatastreamPositionResponse response = DatastreamPositionResponse.fromJson(json);
        _logger.debug("Datastream position response from instance {} is {}", instance, response);
        responseList.add(response);
      } catch (Exception e) {
        _logger.error("Invalid datastream position response {} from instance {}.", json, instance, e);
      }
    });
    DatastreamPositionResponse result = responseList.stream()
        .reduce(DatastreamPositionResponse::merge)
        .orElse(new DatastreamPositionResponse());
    _logger.debug("Final reduced datastream position response {}", result);
    return DatastreamPositionResponse.toJson(result);
  }

  /**
   * Used by the server to query connector about whether certain types of updates are supported for a datastream.
   * Kafka connectors currently support pause/resume of partitions
   * @param datastream the datastream
   * @param updateType Type of datastream update
   * @return true if the connector supports the type of datastream update; false otherwise
   */
  @Override
  public boolean isDatastreamUpdateTypeSupported(Datastream datastream, DatastreamConstants.UpdateType updateType) {
    switch (updateType) {
      case PAUSE_RESUME_PARTITIONS:
        return true;
      default:
        return false;
    }
  }

  /**
   * Extracts the value of a query param (e.g. ?key1=value1&key2=value2...) given a provided request and param key.
   * @param request the request potentially containing query params
   * @param key the param key (i.e. key1 in the example above)
   * @return the value of the param if it exists (i.e. value1 in the example above)
   */
  private Optional<String> extractQueryParam(URI request, String key) {
    if (request == null || request.getQuery() == null) {
      return Optional.empty();
    }
    final List<NameValuePair> pairs = URLEncodedUtils.parse(request.getQuery(), Charset.defaultCharset());
    return Optional.ofNullable(pairs)
        .orElse(Collections.emptyList())
        .stream()
        .filter(pair -> pair.getName().equalsIgnoreCase(key))
        .map(NameValuePair::getValue)
        .findFirst();
  }
}
