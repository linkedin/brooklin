/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.DiagnosticsAware;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.ThreadUtils;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 * Base class for connectors that consume from Kafka.
 *
 * This class abstracts out common logic needed for all Kafka connectors such as:
 * <ul>
 *  <li>Creating and spawning threads to handle {@link DatastreamTask}s assigned to this connector instance</li>
 *  <li>Tracking all currently running tasks and listening for changes in task assignment</li>
 *  <li>Updating the new task assignment (starting/stopping tasks as needed)</li>
 *  <li>Restarting stalled {@link DatastreamTask}s</li>
 * </ul>
 */
public abstract class AbstractKafkaConnector implements Connector, DiagnosticsAware {

  public static final String IS_GROUP_ID_HASHING_ENABLED = "isGroupIdHashingEnabled";

  private static final Duration CANCEL_TASK_TIMEOUT = Duration.ofSeconds(15);
  private static final Duration POST_CANCEL_TASK_TIMEOUT = Duration.ofSeconds(5);
  private static final Duration SHUTDOWN_EXECUTOR_SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
  static final Duration MIN_DAEMON_THREAD_STARTUP_DELAY = Duration.ofMinutes(2);

  private static final String NUM_TASK_RESTARTS = "numTaskRestarts";
  private volatile long _numTaskRestarts = 0;
  private final String _metricsPrefix;

  protected final DynamicMetricsManager _dynamicMetricsManager;
  protected final String _connectorName;
  protected final KafkaBasedConnectorConfig _config;
  protected final GroupIdConstructor _groupIdConstructor;
  protected final String _clusterName;

  private final Logger _logger;
  private final AtomicInteger _threadCounter = new AtomicInteger(0);
  // Access to this map must be synchronized at all times since it can be accessed by multiple concurrent threads.
  private final Map<DatastreamTask, ConnectorTaskEntry> _runningTasks = new HashMap<>();

  // A daemon executor to constantly check whether all tasks are running and restart them if not.
  private final ScheduledExecutorService _daemonThreadExecutorService =
      Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
        @Override
        public Thread newThread(@NotNull Runnable r) {
          Thread t = new Thread(r);
          t.setDaemon(true);
          t.setName(String.format("%s daemon thread", _connectorName));
          return t;
        }
      });

  // An executor to spawn threads to stop tasks, and cancel them if stuck too long in onAssignmentChange().
  private final ExecutorService _shutdownExecutorService = Executors.newCachedThreadPool();

  enum DiagnosticsRequestType {
    DATASTREAM_STATE,
    PARTITIONS,
    CONSUMER_OFFSETS
  }

  /**
   * Constructor for AbstractKafkaConnector.
   * @param connectorName the connector name
   * @param config Kafka-based connector configuration options
   * @param groupIdConstructor Consumer group ID constructor for the Kafka Consumer
   * @param clusterName Brooklin cluster name
   * @see KafkaBasedConnectorConfig
   */
  public AbstractKafkaConnector(String connectorName, Properties config, GroupIdConstructor groupIdConstructor,
      String clusterName, Logger logger) {
    _connectorName = connectorName;
    _logger = logger;
    _clusterName = clusterName;
    _config = new KafkaBasedConnectorConfig(config);
    _groupIdConstructor = groupIdConstructor;
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _metricsPrefix = StringUtils.isBlank(connectorName) ? this.getClass().getSimpleName()
        : connectorName + "." + this.getClass().getSimpleName();
    _dynamicMetricsManager.registerGauge(_metricsPrefix, NUM_TASK_RESTARTS, () -> _numTaskRestarts);
  }

  protected abstract AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task);

  @Override
  public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
    _logger.info("onAssignmentChange called with tasks {}", tasks);

    synchronized (_runningTasks) {
      Set<DatastreamTask> toCancel = new HashSet<>(_runningTasks.keySet());
      toCancel.removeAll(tasks);

      for (DatastreamTask task : toCancel) {
        ConnectorTaskEntry connectorTaskEntry = _runningTasks.remove(task);
        // Spawn a separate thread to attempt stopping the connectorTask. The connectorTask will be canceled if it
        // does not stop within a certain amount of time. This will force cleanup of connectorTasks which take too long
        // to stop, or are stuck indefinitely. A separate thread is spawned to handle this because the Coordinator
        // requires that onAssignmentChange() must complete quickly, and will kill the assignment threads if they take
        // too long.
        _shutdownExecutorService.submit(() -> {
          stopTask(task, connectorTaskEntry);
        });
      }

      for (DatastreamTask task : tasks) {
        ConnectorTaskEntry connectorTaskEntry = _runningTasks.get(task);
        if (connectorTaskEntry != null) {
          AbstractKafkaBasedConnectorTask kafkaBasedConnectorTask = connectorTaskEntry.getConnectorTask();
          kafkaBasedConnectorTask.checkForUpdateTask(task);
          // Make sure to replace the DatastreamTask with most up to date info
          // This is necessary because DatastreamTaskImpl.hashCode() does not take into account all the
          // fields/properties of the DatastreamTask (e.g. dependencies).
          _runningTasks.remove(task);
          _runningTasks.put(task, connectorTaskEntry);
          continue; // already running
        }

        _runningTasks.put(task, createKafkaConnectorTask(task));
      }
    }
  }

  /**
   * Create a thread to run the provided {@link AbstractKafkaBasedConnectorTask} without starting it.
   */
  public Thread createTaskThread(AbstractKafkaBasedConnectorTask task) {
    Thread t = new Thread(task);
    t.setDaemon(true);
    t.setName(
        String.format("%s task thread %s %d", _connectorName, task.getTaskName(), _threadCounter.incrementAndGet()));
    t.setUncaughtExceptionHandler(
        (thread, e) -> _logger.error(String.format("thread %s has died due to uncaught exception.", thread.getName()),
            e));
    return t;
  }

  private ConnectorTaskEntry createKafkaConnectorTask(DatastreamTask task) {
    _logger.info("Creating connector task for datastream task {}.", task);
    AbstractKafkaBasedConnectorTask connectorTask = createKafkaBasedConnectorTask(task);
    Thread taskThread = createTaskThread(connectorTask);
    taskThread.start();
    return new ConnectorTaskEntry(connectorTask, taskThread);
  }

  @Override
  public void start(CheckpointProvider checkpointProvider) {
    _daemonThreadExecutorService.scheduleAtFixedRate(() -> {
      try {
        restartDeadTasks();
      } catch (Exception e) {
        // catch any exceptions here so that subsequent check can continue
        // see java doc of scheduleAtFixedRate
        _logger.warn("Failed to check status of kafka connector tasks.", e);
      }
    }, getThreadDelayTimeInSecond(OffsetDateTime.now(ZoneOffset.UTC), _config.getDaemonThreadIntervalSeconds()),
        _config.getDaemonThreadIntervalSeconds(), TimeUnit.SECONDS);
  }

  /**
   * Check the health of all {@link AbstractKafkaBasedConnectorTask} corresponding to the DatastreamTasks.
   * If any of them are not running or not healthy, Restart them.
   */
  protected void restartDeadTasks() {
    synchronized (_runningTasks) {
      if (_runningTasks.isEmpty()) {
        _logger.info("Connector received no datastreams tasks yet.");
        return;
      }

      _logger.info("Checking the status of {} running connector tasks.", _runningTasks.size());

      Set<DatastreamTask> deadDatastreamTasks = new HashSet<>();
      _runningTasks.forEach((datastreamTask, connectorTaskEntry) -> {
        if (isTaskDead(connectorTaskEntry)) {
          _logger.warn("Detected that the kafka connector task is not running for datastream task {}. Restarting it",
              datastreamTask);
          boolean stopped = stopTask(datastreamTask, connectorTaskEntry);
          if (stopped) {
            deadDatastreamTasks.add(datastreamTask);
          } else {
            _logger.error("Connector task for datastream task {} could not be stopped.", datastreamTask);
          }
        } else {
          _logger.info("Connector task for datastream task {} is healthy", datastreamTask);
        }
      });

      deadDatastreamTasks.forEach(datastreamTask -> {
        _logger.warn("Creating a new connector task for the datastream task {}", datastreamTask);
        _runningTasks.put(datastreamTask, createKafkaConnectorTask(datastreamTask));
      });
      _numTaskRestarts = deadDatastreamTasks.size();
    }
  }

  /**
   * Stop the datastream task and wait for it to stop. If it has not stopped within a timeout, interrupt the thread.
   * @param datastreamTask Datastream task that needs to stopped.
   * @param connectorTaskEntry connectorTaskEntry corresponding to the datastream task.
   * @return true if it was successfully stopped, false if it was not.
   */
  private boolean stopTask(DatastreamTask datastreamTask, ConnectorTaskEntry connectorTaskEntry) {
    try {
      AbstractKafkaBasedConnectorTask connectorTask = connectorTaskEntry.getConnectorTask();
      connectorTask.stop();
      boolean stopped = connectorTask.awaitStop(CANCEL_TASK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      if (!stopped) {
        _logger.warn("Connector task for datastream task {} took longer than {} ms to stop. Interrupting the thread.",
            datastreamTask, CANCEL_TASK_TIMEOUT.toMillis());
        connectorTaskEntry.getThread().interrupt();
        // Check that the thread really got interrupted and log a message if it seems like the thread is still running.
        // Threads which don't check for the interrupt status may land up running forever, and we would like to
        // at least know of such cases for debugging purposes.
        if (!connectorTask.awaitStop(POST_CANCEL_TASK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
          _logger.warn("Connector task for datastream task {} did not stop even after {} ms.", datastreamTask,
              POST_CANCEL_TASK_TIMEOUT.toMillis());
        }
      }
      return true;
    } catch (InterruptedException e) {
      _logger.warn(String.format("Caught exception while trying to stop the connector task for datastream task %s",
          datastreamTask), e);
    }

    return false;
  }

  /**
   * Check if the {@link AbstractKafkaBasedConnectorTask} is dead.
   * @param connectorTaskEntry connector task and thread that needs to be checked whether it is dead.
   * @return true if it is dead, false if it is still running.
   */
  protected boolean isTaskDead(ConnectorTaskEntry connectorTaskEntry) {
    Thread taskThread = connectorTaskEntry.getThread();
    AbstractKafkaBasedConnectorTask connectorTask = connectorTaskEntry.getConnectorTask();
    return (taskThread == null || !taskThread.isAlive()
        || ((System.currentTimeMillis() - connectorTask.getLastPolledTimeMillis())
        >= _config.getNonGoodStateThresholdMillis()));
  }

  @Override
  public void stop() {
    _daemonThreadExecutorService.shutdown();
    synchronized (_runningTasks) {
      // Try to stop the the tasks
      _runningTasks.forEach(this::stopTask);
      _runningTasks.clear();
    }
    _logger.info("Start to shut down the shutdown executor and wait up to {} ms.",
        SHUTDOWN_EXECUTOR_SHUTDOWN_TIMEOUT.toMillis());
    ThreadUtils.shutdownExecutor(_shutdownExecutorService, SHUTDOWN_EXECUTOR_SHUTDOWN_TIMEOUT, _logger);
    _logger.info("Connector stopped.");
  }

  /**
   * This will make the thread delay and fire until a certain timestamp, ex. 6:00, 6:05, 6:10..etc so that threads
   * in different hosts are firing roughly at the same time. The initial delays will be larger than min_initial_delay
   * unless the threads interval is too small, so that a 5:59 task, will not fire at 6:00 but 6:05.
   * @param now the current date time in UTC (exposed for testing)
   * @param daemonThreadIntervalSeconds the frequency of the thread in seconds
   * @return the time thread need to be delayed in second
   */
  @VisibleForTesting
  long getThreadDelayTimeInSecond(OffsetDateTime now, int daemonThreadIntervalSeconds) {
    long truncatedTimestamp = now.truncatedTo(ChronoUnit.HOURS).toEpochSecond();
    long minDelay = Math.min(MIN_DAEMON_THREAD_STARTUP_DELAY.getSeconds(), daemonThreadIntervalSeconds);
    while ((truncatedTimestamp - now.toEpochSecond()) < minDelay) {
      truncatedTimestamp += daemonThreadIntervalSeconds;
    }
    return truncatedTimestamp - now.toEpochSecond();
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

    for (Map.Entry<String, Set<String>> entry : pausedSourcePartitionsMap.entrySet()) {
      String source = entry.getKey();
      Set<String> newPartitions = entry.getValue();

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
      if (path != null && path.equalsIgnoreCase(DiagnosticsRequestType.DATASTREAM_STATE.toString())) {
        String response = processDatastreamStateRequest(uri);
        _logger.trace("Query: {} returns response: {}", query, response);
        return response;
      } else if (path != null && path.equalsIgnoreCase(DiagnosticsRequestType.PARTITIONS.toString())) {
        String response = processTopicPartitionStatsRequest();
        _logger.trace("Query: {} returns response: {}", query, response);
        return response;
      } else if (path != null && path.equalsIgnoreCase(DiagnosticsRequestType.CONSUMER_OFFSETS.toString())) {
        String response = processConsumerOffsetsRequest();
        _logger.trace("Query: {} returns response: {}", query, response);
        return response;
      } else {
        _logger.warn("Could not process query {} with path {}", query, path);
      }
    } catch (Exception e) {
      _logger.warn("Failed to process query {}", query);
      _logger.debug(String.format("Failed to process query %s", query), e);
      throw new DatastreamRuntimeException(e);
    }
    return null;
  }

  /**
   * Collect stats from every running task and join them into single response
   */
  private Optional<KafkaDatastreamStatesResponse> getDatastreamStatesResponse(String streamName) {
    List<KafkaDatastreamStatesResponse> responses;
    synchronized (_runningTasks) {
      responses = _runningTasks.values()
          .stream()
          .map(ConnectorTaskEntry::getConnectorTask)
          .filter(task -> task.hasDatastream(streamName))
          .map(AbstractKafkaBasedConnectorTask::getKafkaDatastreamStatesResponse)
          .collect(Collectors.toList());
    }
    if (responses.size() > 0) {
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions = new HashMap<>();
      Map<String, Set<String>> manualPausedPartitions = new HashMap<>();
      Set<TopicPartition> assignedTopicPartitions = new HashSet<>();
      responses.forEach(t -> {
        autoPausedPartitions.putAll(t.getAutoPausedPartitions());
        manualPausedPartitions.putAll(t.getManualPausedPartitions());
        assignedTopicPartitions.addAll(t.getAssignedTopicPartitions());
      });
      return Optional.of(new KafkaDatastreamStatesResponse(responses.get(0).getDatastream(), autoPausedPartitions,
          manualPausedPartitions, assignedTopicPartitions));
    }
    return Optional.empty();
  }

  private String processDatastreamStateRequest(URI request) {
    _logger.info("process Datastream state request: {}", request);
    Optional<String> datastreamName = extractQueryParam(request, DATASTREAM_KEY);
    return datastreamName.flatMap(this::getDatastreamStatesResponse)
        .map(KafkaDatastreamStatesResponse::toJson).orElse(null);
  }

  private String processTopicPartitionStatsRequest() {
    _logger.info("process topic partitions stats request");
    List<KafkaTopicPartitionStatsResponse> serializedResponses = new ArrayList<>();

    synchronized (_runningTasks) {
      _runningTasks.forEach((dataStreamTask, connectorTaskEntry) -> {
        List<Datastream> datastreams = dataStreamTask.getDatastreams();
        Set<String> datastreamNames = new HashSet<>();
        datastreams.forEach(datastream -> datastreamNames.add(datastream.getName()));

        KafkaTopicPartitionTracker tracker = connectorTaskEntry.getConnectorTask().getKafkaTopicPartitionTracker();

        String hostname = null;
        try {
          hostname = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException ex) {
          _logger.warn("Hostname not found");
        }

        KafkaTopicPartitionStatsResponse response = new KafkaTopicPartitionStatsResponse(tracker.getConsumerGroupId(),
                hostname, tracker.getTopicPartitions(), datastreamNames);
        serializedResponses.add(response);
      });
    }
    return JsonUtils.toJson(serializedResponses);
  }

  private String processConsumerOffsetsRequest() {
    _logger.info("process consumer stats request");
    List<KafkaConsumerOffsetsResponse> serializedResponses = new ArrayList<>();

    synchronized (_runningTasks) {
      _runningTasks.forEach((datastreamTask, connectorTaskEntry) -> {
        KafkaTopicPartitionTracker tracker = connectorTaskEntry.getConnectorTask().getKafkaTopicPartitionTracker();
        KafkaConsumerOffsetsResponse response = new KafkaConsumerOffsetsResponse(tracker.getConsumerOffsets(),
            tracker.getConsumerGroupId());
        serializedResponses.add(response);
      });
    }
    return JsonUtils.toJson(serializedResponses);
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
    _logger.info("Reducing query {} with responses from {} instances", query, responses.size());
    _logger.debug("Reducing query {} with responses from {}", query, responses.keySet());
    _logger.trace("Reducing query {} with responses {}", query, responses);
    try {
      String path = getPath(query, _logger);
      if (path != null
          && (path.equalsIgnoreCase(DiagnosticsRequestType.DATASTREAM_STATE.toString()))) {
        return JsonUtils.toJson(responses);
      } else if (path != null
          && (path.equalsIgnoreCase(DiagnosticsRequestType.PARTITIONS.toString()))) {
        return KafkaConnectorDiagUtils.reduceTopicPartitionStatsResponses(responses, _logger);
      } else if (path != null
          && (path.equalsIgnoreCase(DiagnosticsRequestType.CONSUMER_OFFSETS.toString()))) {
        return KafkaConnectorDiagUtils.reduceConsumerOffsetsResponses(responses, _logger);
      }
    } catch (Exception e) {
      _logger.warn("Failed to reduce responses from query {}: {}", query, e.getMessage());
      _logger.debug(String.format("Failed to reduce responses from query %s: %s", query, e.getMessage()), e);
      _logger.trace(String.format("Failed to reduce responses %s from query %s: %s", responses, query, e.getMessage()), e);
      throw new DatastreamRuntimeException(e);
    }
    return null;
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
    return updateType == DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS;
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

  protected static class ConnectorTaskEntry {
    private final AbstractKafkaBasedConnectorTask _task;
    private final Thread _thread;

    public ConnectorTaskEntry(AbstractKafkaBasedConnectorTask connectorTask, Thread connectorTaskThread) {
      _task = connectorTask;
      _thread = connectorTaskThread;
    }

    public Thread getThread() {
      return _thread;
    }

    public AbstractKafkaBasedConnectorTask getConnectorTask() {
      return _task;
    }
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.add(new BrooklinGaugeInfo(buildMetricName(_metricsPrefix, NUM_TASK_RESTARTS)));

    return Collections.unmodifiableList(metrics);
  }
}
