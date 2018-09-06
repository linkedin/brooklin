package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Map;
import java.util.List;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.SchemaRegistryClient;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleConsumer;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleConsumerConfig;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleChangeEvent;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import static java.lang.String.format;


/**
 * This is the Handler class that handles each datastreamTask. The handler manages
 * two main classes. The OracleConsumer and the MessageHandler.
 *
 * 1. The OracleConsumer will query the source speicified in the datastream in order
 *    to grab all the changed rows with SCN greater than the current {@code _lastScn}.
 *    That ResultSet is then converted into {@code List<OracleChangeEvent>} which is
 *    simply an abstract representation of the each row that was changed.
 *
 * 2. The Message Handler will take each OracleChangeEvent and pass it onto the
 *    TransportProvider as specified in the datastream. The message handler is
 *    responsible for packaging each OracleChangeEvent into a DatastreamEvent
 *
 * The OracleTaskHandler maintains an {@code _lastScn} which it passes onto the
 * OracleConsumer. The {@code _lasScn} represents the checkpoint of how many changes
 * have already been processed. This {@code _lastScn} is held in Zookeeper to handle
 * crash scenarios
 *
 * Note: Other Connectors may have specific logic to handle change events that are
 *       part of a larger transaction. This is because changes from the source could
 *       be reverted or aborted. In the case of the TriggerBased Connector, the
 *       changeEvents generated have all been successfully committed.
 *
 ┌─────────────────────────────────────────────────────────────────┐
 │    OracleTaskHandler 1                                          │
 │   ┌──────────────────┐           ┌─────────────────────┐        │
 │                                                                 │
 │   │ OracleConsumer 1 │──────────▶│  MessageHandler 1   │        │
 │   │                  │           │                     │        │
 │   └──────────────────┘           └─────────────────────┘        │
 │             ▲                               │                   │
 │             │                               │                   │
 └─────────────┼───────────────────────────────┼───────────────────┘
         ┌────▼────┐                          │
         │         │          ┌───────────────▼──────────────────┐
         │         │          │                                  │
         │         │          │        TransportProvider         │
         │Oracle DB│          │                                  │
         │         │          └──────────────────────────────────┘
         │         │
         └─────────┘
 */

public class OracleTaskHandler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(OracleTaskHandler.class);
  // since there is not re-partitioning in the Oracle Connector, we default all partition
  // references to 0
  private static final int PARTITION_NUM = 0;
  private static final String CLASS_NAME = OracleTaskHandler.class.getSimpleName();
  private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofSeconds(4);
  private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ofSeconds(12);
  private static final Duration REPORT_INTERVAL = Duration.ofMinutes(1);

  // Regular expression to capture all metrics starting with OracleTaskHandler.TOPIC_NAME.
  private static final String TOPIC_METRICS_PREFIX_REGEX =
      OracleTaskHandler.class.getSimpleName() + MetricsAware.KEY_REGEX;

  private final String _handlerName;
  private volatile boolean _shutdown;

  private final OracleSource _source;
  private final MessageHandler _messageHandler;

  private OracleConsumer _oracleConsumer;
  private long _lastScn;
  private final DatastreamTask _task;
  private final OracleTBMetrics _metrics;
  private final ExecutorService _executorService;
  private Future<?> _taskHandlerThreadStatus;
  private final Schema _schema;

  private int _eventsProcessedCountLog = 0;
  private Instant _lastReportTime = Instant.now();


  public OracleTaskHandler(OracleSource source,
      ExecutorService executorService,
      SchemaRegistryClient liKafkaSchemaRegistry,
      DatastreamTask task) {

    _handlerName = task.getDatastreamTaskName();
    _executorService = executorService;
    _shutdown = false;
    _source = source;
    _task = task;

    _schema = SchemaUtil.getSchemaByTask(task, liKafkaSchemaRegistry);
    _messageHandler = new MessageHandler(source, _schema, task);

    // initialize metrics
    _metrics = new OracleTBMetrics(_handlerName);
  }

  public void initializeConsumer(OracleConsumerConfig consumerConfig) throws DatastreamException {

    _oracleConsumer = new OracleConsumer(consumerConfig, _source, _schema);

    try {
      _oracleConsumer.initializeConnection();
    } catch (DatastreamException e) {
      LOG.error("Failed to create OracleConsumer for source: {}", _source.getConnectionString());
      throw e;
    }
  }

  public void stop() {
    LOG.info("Shutdown requested for OracleTaskHandler. EventReaderName: " + _handlerName);
    _shutdown = true;
  }

  /**
   * If a task Handler thread throws an exception or exits for any unexpected reason, there is a daemon
   * running to ensure that threads are restarted. We need to re-obtain the safe checkpoint when we are
   * restarting a thread
   */
  public void start() throws DatastreamException {
    LOG.info("Starting OracleTaskHandler: {}", _handlerName);

    try {
      _lastScn = getSafeCheckpoint();
    } catch (DatastreamException e) {
      LOG.error(String.format("OracleTaskHandler: %s failed to get starting _lastScn: ", _handlerName), e);
      throw e;
    }

    _taskHandlerThreadStatus = _executorService.submit(this);
  }

  public synchronized void restartIfNotRunning() {
    if (isHandlerRunning()) {
      return;
    }

    LOG.warn("Detected that OracleTaskHandler {} is not running! Restarting...", _handlerName);

    try {
      start();
    } catch (DatastreamException e) {
      // daemon will attempt to restart this thread again after a certain wait time.
      LOG.error(String.format("Failed to restart OracleTaskHandler: %s", _handlerName), e);
    }
  }

  @Override
  public void run() {
    LOG.info("Start to run OracleTaskHandler, HandlerName: {}", _handlerName);
    Instant lastStartTime = Instant.now();

    try {
      while (!_shutdown) {
        // This Oracle Connector does not support Bootstrapping, therefore we dont support _lastScn == 0
        if (_lastScn == 0) {
          throw new DatastreamException("Failed to get checkpoint _lastScn: " + _lastScn);
        }

        // report to log, ensuring the system is operating as expected
        periodicallyReport();

        // Sleep for some preset time to prevent overloading the Oracle servers
        // TODO (sdkhan) move this over to an exponential backoff policy
        try {
          Thread.sleep(4000);
        } catch (InterruptedException e) {
          LOG.error(String.format("OracleTaskHandler: %s failed to sleep", _handlerName), e);
          // do nothing
        }

        // record the time in between 2 separate polling sessions. This is essentially the time
        // it takes to process the messages after the query has been completed.
        Instant startTime = Instant.now();
        Duration timeSinceLastPoll = Duration.between(lastStartTime, startTime);
        lastStartTime = startTime;

        if (timeSinceLastPoll.compareTo(DEFAULT_SESSION_TIMEOUT) > 0) {
          LOG.warn("OracleTaskHandler: {}, Time since last Oracle Poll is {} ms (> session timeout {} ms)",
              _handlerName,
              timeSinceLastPoll.toMillis(),
              DEFAULT_SESSION_TIMEOUT.toMillis());
        }

        List<OracleChangeEvent> changeEvents;

        try {
          changeEvents = _oracleConsumer.consume(_lastScn);
        } catch (SQLException e) {
          LOG.error(format("Querying for changes failed with source: %s and lastScn: %d",
              _source.getViewName(),
              _lastScn), e);
          throw e;
        }

        _metrics.incrementPollCount();
        _metrics.updateEventCountPerPoll(changeEvents.size());

        Duration pollTime = Duration.between(startTime, Instant.now());

        // record issue if pollTime was too large
        if (pollTime.compareTo(DEFAULT_POLL_TIMEOUT) > 0) {
          LOG.warn("OracleTaskHandler: {} took {} to ms to query Oracle Database (> {} ms poll timeout)",
              _handlerName,
              pollTime.toMillis(),
              DEFAULT_POLL_TIMEOUT.toMillis());
        }

        // skip to the next iteration if the there was 0 ChangeEvents
        if (changeEvents.size() == 0) {
          LOG.debug("OracleTaskHandler: {} found 0 OracleChangeEvents using lastScn: {}", _handlerName, _lastScn);
          continue;
        }

        Instant processingStartTime = Instant.now();
        Iterator<OracleChangeEvent> iterator = changeEvents.iterator();
        iterator.forEachRemaining(this::processChangeEvent);

        _metrics.incrementProcessingRate();
        _metrics.updateEventToProcessedLatency(changeEvents);
        _eventsProcessedCountLog += changeEvents.size();

        Duration processingTime = Duration.between(processingStartTime, Instant.now());
        LOG.debug("OracleTaskHandler: {} processing {} took {}(ms)", _handlerName, changeEvents.size(),
            processingTime.toMillis());

        // If we got here, then we have successfully processed all the events
        // returned by the Poll. Now we have to update the {@code _lastScn}
        updateLastScn(changeEvents);
      }
    } catch (Throwable t) {
      LOG.error(format("OracleTaskHandler: %s exited abnormally.", _handlerName), t);
    } finally {
      _oracleConsumer.closeConnection();
      LOG.info("OracleTaskHandler is shutdown. OracleTaskHandler: " + _handlerName);
    }
  }

  /**
   * The OracleConsumer will return a list of OracleChangeEvents that contain data
   * of the all the rows that have been changed from a specific view. This
   * function will process each event and find the corresponding MessageHandler
   *
   * @param changeEvent - a Single OracleChangeEvent that needs to be sent to the transport provider
   */
  private void processChangeEvent(OracleChangeEvent changeEvent) {
    String viewName = _source.getViewName();

    try {
      _messageHandler.processChangeEvent(changeEvent);

      // Check to see if there is an error from the downstream Transport Provider
      if (_messageHandler.isErrorState()) {
        Map<Integer, String> checkpointMap = _task.getCheckpoints();

        // If we fail to get a checkpoint then throw an Error
        // This should not happen
        // TODO (sdkhan) store an {@code _oldScn} incase checkpointing fails
        if (checkpointMap == null || checkpointMap.get(PARTITION_NUM).equals("")) {
          throw new DatastreamRuntimeException(format("No checkpoint found for OracleTaskHandler: %s", _handlerName));
        }

        // update the _lastScn to the checkpoint Value
        // this implies that we will reprocess events but
        // maintain order
        String checkpoint = checkpointMap.get(PARTITION_NUM);
        _lastScn = Long.parseLong(checkpoint);

        LOG.info("OracleTaskHandler: {} reset the lastScn to: {}", _handlerName, _lastScn);

        _messageHandler.resetErrorState();
      }

    } catch (DatastreamException e) {
      String msg = format("OracleTaskHandler: %s failed to process OracleChangeEvent: %s",
          _handlerName,
          changeEvent.toString());

      _metrics.incrementErrorRate();
      LOG.error(msg, e);

      // We throw a RuntimeException here because we dont want update the
      // {@code _lastScn} unless we have successfully passed the event into
      // the transport provider (that would invalidate our ordered events
      // gauruntee)
      throw new RuntimeException(e);
    } catch (Exception e) {
      String msg = String.format("OracleTaskHandler: %s failed to restart from checkpoint after Transport Provider error. _lastScn: %d",
          _handlerName,
          _lastScn);

      LOG.error(msg, e);
      throw new RuntimeException(e);
    }

  }

  /**
   * Determine if this current thread is running.
   */
  private boolean isHandlerRunning() {
    return _taskHandlerThreadStatus != null && !_taskHandlerThreadStatus.isDone();
  }


  /**
   * retrieve a safe checkpoint to give the OracleTaskHandler a starting point.
   * There is one OracleTaskHandler per running DatastreamTask, hence if this is a
   * new DatastreamTask, the checkpoint will return nothing. In that situation
   * we grab the highest SCN in the TxLog and start from there.
   *
   * @throws DatastreamException - if this fails, restart the OracleTaskHandler
   */
  private long getSafeCheckpoint() throws DatastreamException {
    // mapping between partition and checkpoint
    Map<Integer, String> checkpointMap = _task.getCheckpoints();

    // safe checkpoint returned nothing, this means that this is
    // a brand new datastreamTask and we want to start processing from
    // the latest SCN
    if (checkpointMap == null || checkpointMap.size() == 0) {
      LOG.info("OracleTaskHandler: {}, did not find Checkpoint, finding the latest SCN", _handlerName);
      long scn;

      try {
        scn = _oracleConsumer.getLatestScn();
      } catch (SQLException e) {
        LOG.error(format("OracleTaskHandler %s failed to get maxScn for new DatastreamTask", _handlerName), e);
        throw new DatastreamException(e);
      }

      return scn;
    }

    // since there is no concept of partitioning in the TriggerBased Oracle Connector
    // we hard code the key to be 0
    String checkpoint = checkpointMap.get(PARTITION_NUM);
    long scn = Long.parseLong(checkpoint);
    LOG.info("OracleTaskHandler: {} Found Checkpoint, lastScn: {}", _handlerName, scn);

    return scn;
  }

  /**
   * After processing the OracleChangeEvents we need to update the
   * {@code _lastScn} so that next time we run the query we dont reprocess
   * the same changeEvents
   *
   * The list of OracleChangeEvents is guaranteed to be sorted by SCN of the
   * individual OracleChangeEvent. Therefore the {@code _lastScn} should be
   * the last SCN from the list of OracleChangeEvents
   *
   * @param events the events returned from the consumer query ordered by SCN
   */
  private void updateLastScn(List<OracleChangeEvent> events) {
    long oldScn = _lastScn;
    _lastScn = events.get(events.size() - 1).getScn();

    LOG.debug("OracleTaskHandler: {} updated its lastScn from old: {} to new: {}", _handlerName, oldScn, _lastScn);
  }

  /**
   * To ensure that the system is working as expected and to aid in debugging, the OracleTaskHandler
   * will periodically log some stats
   */
  private void periodicallyReport() {
    // ensure at least some time has passed
    Duration timeSinceLastReport = Duration.between(_lastReportTime, Instant.now());
    if (timeSinceLastReport.compareTo(REPORT_INTERVAL) < 0) {
      return;
    }

    // log info
    String msg = format("OracleTaskHandler: %s. Processed %s events in %s ms",
        _handlerName,
        _eventsProcessedCountLog,
        timeSinceLastReport.toMillis());
    LOG.info(msg);

    // reset counts and timers
    _eventsProcessedCountLog = 0;
    _lastReportTime = Instant.now();
  }

  static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();

    metrics.add(new BrooklinMeterInfo(TOPIC_METRICS_PREFIX_REGEX + OracleTBMetrics.POLL_COUNT_KEY));
    metrics.add(new BrooklinMeterInfo(TOPIC_METRICS_PREFIX_REGEX + OracleTBMetrics.PROCESSING_RATE_KEY));
    metrics.add(new BrooklinMeterInfo(TOPIC_METRICS_PREFIX_REGEX + OracleTBMetrics.ERROR_RATE_KEY));
    metrics.add(new BrooklinHistogramInfo(TOPIC_METRICS_PREFIX_REGEX + OracleTBMetrics.EVENT_TO_PROCESSED_LATENCY_KEY));
    metrics.add(new BrooklinHistogramInfo(TOPIC_METRICS_PREFIX_REGEX + OracleTBMetrics.EVENT_COUNT_PER_POLL_KEY));

    return metrics;
  }


  private static class OracleTBMetrics {
    private final DynamicMetricsManager _metricManager;
    private final String _name;

    private static final String PROCESSING_RATE_KEY = "processingRate";
    private static final String EVENT_COUNT_PER_POLL_KEY = "eventCountPerPoll";
    private static final String ERROR_RATE_KEY = "errorRate";
    private static final String POLL_COUNT_KEY = "pollCount";
    private static final String AGGREGATE_KEY = "aggregate";
    private static final String EVENT_TO_PROCESSED_LATENCY_KEY = "eventToProcessedLatency";


    /* Per OracleTaskHandler Metrics */
    // The number of events retrieved in one poll
    private final Histogram _eventCountPerPoll;
    // The latency, time between when the row was changed to when it was finished processing
    private final Histogram _eventToProcessedLatency;
    // The rate at which we can process changes end to end
    private final Meter _processingRate;
    // THe rate at which we receive errors while trying to process changes end to end
    private final Meter _errorRate;
    // The number of polls from the OracleConsumer
    private final Meter _pollCount;


    /* Aggregate Metrics */
    private static Meter _aggregatedProcessingRate;
    private static Meter _aggregatedErrorRate;
    private static Histogram _aggregatedEventToProcessedLatency;


    private OracleTBMetrics(String name) {
      _name = name;
      _metricManager = DynamicMetricsManager.getInstance();

      _processingRate = _metricManager.registerMetric(CLASS_NAME, _name, PROCESSING_RATE_KEY, Meter.class);
      _eventCountPerPoll = _metricManager.registerMetric(CLASS_NAME, _name, EVENT_COUNT_PER_POLL_KEY, Histogram.class);
      _errorRate = _metricManager.registerMetric(CLASS_NAME, _name, ERROR_RATE_KEY, Meter.class);
      _pollCount = _metricManager.registerMetric(CLASS_NAME, _name, POLL_COUNT_KEY, Meter.class);
      _eventToProcessedLatency = _metricManager.registerMetric(CLASS_NAME, _name, EVENT_TO_PROCESSED_LATENCY_KEY,
          Histogram.class);

      // Register Aggregated metric if not registered by another instance.
      _aggregatedErrorRate = _metricManager.registerMetric(CLASS_NAME, AGGREGATE_KEY, ERROR_RATE_KEY, Meter.class);
      _aggregatedProcessingRate = _metricManager.registerMetric(CLASS_NAME, AGGREGATE_KEY, PROCESSING_RATE_KEY, Meter.class);
      _aggregatedEventToProcessedLatency = _metricManager.registerMetric(CLASS_NAME, AGGREGATE_KEY,
          EVENT_TO_PROCESSED_LATENCY_KEY, Histogram.class);
    }

    private void incrementPollCount() {
      _pollCount.mark(1);
    }

    private void updateEventCountPerPoll(int val) {
      _eventCountPerPoll.update(val);
    }

    private void incrementErrorRate() {
      _aggregatedErrorRate.mark(1);
      _errorRate.mark(1);
    }

    private void incrementProcessingRate() {
      _aggregatedProcessingRate.mark(1);
      _processingRate.mark(1);
    }

    /**
     * Record the time between the sourceTimestamp (the timestamp of when the change occured)
     * and now (after processing has been completed).
     */
    private void updateEventToProcessedLatency(List<OracleChangeEvent> events) {
      long current = System.currentTimeMillis();
      for (OracleChangeEvent event : events) {
        long latency = current - event.getSourceTimestamp();
        _eventToProcessedLatency.update(latency);
        _aggregatedEventToProcessedLatency.update(latency);
      }
    }
  }
}
