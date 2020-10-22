/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.SendFailedException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.providers.NoOpCheckpointProvider;

/**
 * EventProducer class uses a {@link TransportProvider}to send events to the destination.
 */
public class EventProducer implements DatastreamEventProducer {

  public static final String CFG_SKIP_MSG_SERIALIZATION_ERRORS = "skipMessageOnSerializationErrors";
  public static final String DEFAULT_SKIP_MSG_SERIALIZATION_ERRORS = "false";
  public static final String CONFIG_FLUSH_INTERVAL_MS = "flushIntervalMs";
  public static final String CONFIG_ENABLE_PER_TOPIC_METRICS = "enablePerTopicMetrics";
  public static final String CONFIG_ENABLE_PER_TOPIC_EVENT_LATENCY_METRICS = "enablePerTopicEventLatencyMetrics";

  // Default flush interval, It is intentionally kept at low frequency. If a particular connectors wants
  // a more frequent flush (high traffic connectors), it can perform that on it's own.
  public static final String DEFAULT_FLUSH_INTERVAL_MS = String.valueOf(Duration.ofMinutes(5).toMillis());

  static final String EVENTS_LATENCY_MS_STRING = "eventsLatencyMs";
  static final String EVENTS_SEND_LATENCY_MS_STRING = "eventsSendLatencyMs";

  private static final String MODULE = EventProducer.class.getSimpleName();
  private static final String METRICS_PREFIX = MODULE + MetricsAware.KEY_REGEX;
  private static final AtomicInteger PRODUCER_ID_SEED = new AtomicInteger(0);
  private static final String TOTAL_EVENTS_PRODUCED = "totalEventsProduced";
  private static final String EVENTS_PRODUCED_WITHIN_SLA = "eventsProducedWithinSla";
  private static final String EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA = "eventsProducedWithinAlternateSla";
  private static final String EVENT_PRODUCE_RATE = "eventProduceRate";
  private static final String FLUSH_LATENCY_MS_STRING = "flushLatencyMs";
  private static final String AVAILABILITY_THRESHOLD_SLA_MS = "availabilityThresholdSlaMs";
  private static final String AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS = "availabilityThresholdAlternateSlaMs";
  private static final String WARN_LOG_LATENCY_ENABLED = "warnLogLatencyEnabled";
  private static final String WARN_LOG_LATENCY_THRESHOLD_MS = "warnLogLatencyThresholdMs";
  private static final String NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_ENABLED = "numEventsOutsideAltSlaLogEnabled";
  private static final String NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_FREQUENCY_MS = "numEventsOutsideAltSlaFrequencyMs";
  private static final String EVENTS_PRODUCED_OUTSIDE_SLA = "eventsProducedOutsideSla";
  private static final String EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA = "eventsProducedOutsideAlternateSla";
  private static final String DROPPED_SENT_FROM_SERIALIZATION_ERROR = "droppedSentFromSerializationError";
  private static final String AGGREGATE = "aggregate";
  private static final String DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS = "60000"; // 1 minute
  private static final String DEFAULT_AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS = "180000"; // 3 minutes
  private static final String DEFAULT_WARN_LOG_LATENCY_ENABLED = "false";
  private static final String DEFAULT_WARN_LOG_LATENCY_THRESHOLD_MS = "1500000000"; // 25000 minutes, ~17 days
  private static final String DEFAULT_NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_ENABLED = "false";
  private static final String DEFAULT_NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_FREQUENCY_MS = "300000"; // 5 minutes
  private static final long LATENCY_SLIDING_WINDOW_LENGTH_MS = Duration.ofMinutes(3).toMillis();
  private static final long LONG_FLUSH_WARN_THRESHOLD_MS = Duration.ofMinutes(5).toMillis();

  private final DatastreamTask _datastreamTask;
  private final int _producerId;
  private final Logger _logger;
  private final TransportProvider _transportProvider;
  private final CheckpointProvider _checkpointProvider;
  private final DynamicMetricsManager _dynamicMetricsManager;
  private final int _availabilityThresholdSlaMs;
  // Alternate SLA for comparison with the main SLA
  private final int _availabilityThresholdAlternateSlaMs;
  // Whether to enable warning logs if the latency threshold is met
  private final boolean _warnLogLatencyEnabled;
  // Latency threshold at which to log a warning message
  private final long _warnLogLatencyThresholdMs;
  // Whether to enable logging the list of TopicPartitions with events outside alternate SLA
  private final boolean _numEventsOutsideAltSlaLogEnabled;
  // Frequency at which to log the list of TopicPartitions with events outside alternate SLA
  private final long _numEventsOutsideAltSlaFrequencyMs;
  private final boolean _skipMessageOnSerializationErrors;
  private final boolean _enablePerTopicMetrics;
  private final boolean _enablePerTopicEventLatencyMetrics;
  private final Duration _flushInterval;

  private Instant _lastFlushTime = Instant.now();
  private long _lastEventsOutsideAltSlaLogTimeMs = System.currentTimeMillis();
  private Map<TopicPartition, Integer> _trackEventsOutsideAltSlaMap = new HashMap<>();

  /**
   * Construct an EventProducer instance.
   * @param transportProvider the transport provider
   * @param checkpointProvider the checkpoint provider
   * @param config the config options
   * @param customCheckpointing decides whether producer should use custom checkpointing or the datastream server
   *                            provided checkpointing.
   */
  public EventProducer(DatastreamTask task, TransportProvider transportProvider, CheckpointProvider checkpointProvider,
      Properties config, boolean customCheckpointing) {
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(config, "null config");

    _datastreamTask = task;
    _transportProvider = transportProvider;
    _producerId = PRODUCER_ID_SEED.getAndIncrement();
    _logger = LoggerFactory.getLogger(String.format("%s:%d", MODULE, _producerId));

    if (customCheckpointing) {
      _checkpointProvider = new NoOpCheckpointProvider();
    } else {
      _checkpointProvider = checkpointProvider;
    }

    _skipMessageOnSerializationErrors = getSkipMessageOnSerializationErrors(task, config);

    _availabilityThresholdSlaMs =
        Integer.parseInt(config.getProperty(AVAILABILITY_THRESHOLD_SLA_MS, DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS));

    _availabilityThresholdAlternateSlaMs = Integer.parseInt(
        config.getProperty(AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS, DEFAULT_AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS));

    _warnLogLatencyEnabled =
        Boolean.parseBoolean(config.getProperty(WARN_LOG_LATENCY_ENABLED, DEFAULT_WARN_LOG_LATENCY_ENABLED));

    _warnLogLatencyThresholdMs =
        Long.parseLong(config.getProperty(WARN_LOG_LATENCY_THRESHOLD_MS, DEFAULT_WARN_LOG_LATENCY_THRESHOLD_MS));

    _numEventsOutsideAltSlaLogEnabled = Boolean.parseBoolean(config.getProperty(NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_ENABLED,
        DEFAULT_NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_ENABLED));

    _numEventsOutsideAltSlaFrequencyMs = Long.parseLong(config.getProperty(NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_FREQUENCY_MS,
        DEFAULT_NUM_EVENTS_OUTSIDE_ALT_SLA_LOG_FREQUENCY_MS));

    _flushInterval =
        Duration.ofMillis(Long.parseLong(config.getProperty(CONFIG_FLUSH_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL_MS)));

    _enablePerTopicMetrics =
        Boolean.parseBoolean(config.getProperty(CONFIG_ENABLE_PER_TOPIC_METRICS, Boolean.TRUE.toString()));

    _enablePerTopicEventLatencyMetrics =
        Boolean.parseBoolean(config.getProperty(CONFIG_ENABLE_PER_TOPIC_EVENT_LATENCY_METRICS,
            Boolean.FALSE.toString()));

    _logger.info("Created event producer with customCheckpointing={}", customCheckpointing);

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    // provision some metrics to force them to create
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, EVENTS_PRODUCED_OUTSIDE_SLA, 0);
    if (!_enablePerTopicMetrics) {
      _dynamicMetricsManager.createOrUpdateCounter(MODULE, getDatastreamName(), EVENTS_PRODUCED_OUTSIDE_SLA, 0);
    }
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(),
        EVENTS_PRODUCED_OUTSIDE_SLA, 0);
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(),
        DROPPED_SENT_FROM_SERIALIZATION_ERROR, 0);
  }

  /**
   * Uses the checkpoint provider to retrieve the committed checkpoints for the given datastream task
   * @param task the datastream task
   * @return a map of partitions to their committed checkpoints
   */
  public Map<Integer, String> loadCheckpoints(DatastreamTask task) {
    return _checkpointProvider.getCommitted(task);
  }

  private void validateEventRecord(DatastreamProducerRecord record) {
    Validate.notNull(record, "null event record.");
    Validate.notNull(record.getEvents(), "null event payload.");
    Validate.notNull(record.getCheckpoint(), "null event checkpoint.");

    for (Object event : record.getEvents()) {
      BrooklinEnvelope envelope = (BrooklinEnvelope) event;
      Validate.notNull(envelope, "null event");
    }
  }

  /**
   * Send the event onto the underlying transport.
   * @param record the datastream event
   * @param sendCallback the callback to be invoked after the event is sent to the destination
   */
  @Override
  public void send(DatastreamProducerRecord record, SendCallback sendCallback) {
    try {
      validateEventRecord(record);

      try {
        record.serializeEvents(_datastreamTask.getDestinationSerDes());
      } catch (Exception e) {
        if (_skipMessageOnSerializationErrors) {
          _logger.info(String.format("Skipping the message on serialization error as configured. "
                  + "Datastream name: %s, Datastream task name: %s",
              getDatastreamName(), _datastreamTask.getDatastreamTaskName()), e);
          _dynamicMetricsManager.createOrUpdateCounter(MODULE, getDatastreamName(),
              DROPPED_SENT_FROM_SERIALIZATION_ERROR, 1);
          _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, DROPPED_SENT_FROM_SERIALIZATION_ERROR, 1);
          return;
        }
        throw e;
      }

      // Send the event to the transport
      String destination =
          record.getDestination().orElse(_datastreamTask.getDatastreamDestination().getConnectionString());
      record.setEventsSendTimestamp(System.currentTimeMillis());
      long recordEventsSourceTimestamp = record.getEventsSourceTimestamp();
      long recordEventsSendTimestamp = record.getEventsSendTimestamp().orElse(0L);
      _transportProvider.send(destination, record,
          (metadata, exception) -> onSendCallback(metadata, exception, sendCallback, recordEventsSourceTimestamp,
              recordEventsSendTimestamp));
    } catch (Exception e) {
      String errorMessage = String.format("Failed send the event %s exception %s", record, e);
      _logger.warn(errorMessage, e);
      throw new DatastreamRuntimeException(errorMessage, e);
    }

    // Force a periodic flush, in case connector is not calling flush at regular intervals
    if (Instant.now().isAfter(_lastFlushTime.plus(_flushInterval))) {
      flush();
    }
  }

  // Report SLA metrics for aggregate, connector and task
  private void reportSLAMetrics(String topicOrDatastreamName, boolean isWithinSLA, String metricNameForWithinSLA,
      String metricNameForOutsideSLA) {
    int withinSLAValue = isWithinSLA ? 1 : 0;
    int outsideSLAValue = isWithinSLA ? 0 : 1;
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, metricNameForWithinSLA, withinSLAValue);
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(), metricNameForWithinSLA,
        withinSLAValue);
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, topicOrDatastreamName, metricNameForWithinSLA, withinSLAValue);
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, metricNameForOutsideSLA, outsideSLAValue);
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(), metricNameForOutsideSLA,
        outsideSLAValue);
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, topicOrDatastreamName, metricNameForOutsideSLA,
        outsideSLAValue);
  }

  private void performSlaRelatedLogging(DatastreamRecordMetadata metadata, long eventsSourceTimestamp,
      long sourceToDestinationLatencyMs) {
    if (_warnLogLatencyEnabled && (sourceToDestinationLatencyMs > _warnLogLatencyThresholdMs)) {
      _logger.warn("Source to destination latency {} ms is higher than {} ms, Datastream: {}, Source Timestamp: {}, "
              + "Metadata: {}", sourceToDestinationLatencyMs, _warnLogLatencyThresholdMs, getDatastreamName(),
          eventsSourceTimestamp, metadata);
    }

    if (_numEventsOutsideAltSlaLogEnabled) {
      if (sourceToDestinationLatencyMs > _availabilityThresholdAlternateSlaMs) {
        TopicPartition topicPartition = new TopicPartition(metadata.getTopic(), metadata.getSourcePartition());
        int numEvents = _trackEventsOutsideAltSlaMap.getOrDefault(topicPartition, 0);
        _trackEventsOutsideAltSlaMap.put(topicPartition, numEvents + 1);
      }

      long timeSinceLastLog = System.currentTimeMillis() - _lastEventsOutsideAltSlaLogTimeMs;
      if (timeSinceLastLog >= _numEventsOutsideAltSlaFrequencyMs) {
        _trackEventsOutsideAltSlaMap.forEach((topicPartition, numEvents) ->
            _logger.warn("{} had {} event(s) with latency greater than alternate SLA of {} ms in the last {} ms for "
                    + "datastream {}", topicPartition, numEvents, _availabilityThresholdAlternateSlaMs,
                timeSinceLastLog, getDatastreamName()));
        _trackEventsOutsideAltSlaMap.clear();
        _lastEventsOutsideAltSlaLogTimeMs = System.currentTimeMillis();
      }
    }
  }

  /**
   * Report metrics on every send callback from the transport provider. Because this can be invoked multiple times
   * per DatastreamProducerRecord (i.e. by the number of events within the record), only increment all metrics by 1
   * to avoid overcounting.
   */
  private void reportMetrics(DatastreamRecordMetadata metadata, long eventsSourceTimestamp, long eventsSendTimestamp) {
    // If per-topic metrics are enabled, use topic as key for metrics; else, use datastream name as the key
    String datastreamName = getDatastreamName();
    String topicOrDatastreamName = _enablePerTopicMetrics ? metadata.getTopic() : datastreamName;
    // Treat all events within this record equally (assume same timestamp)
    if (eventsSourceTimestamp > 0) {
      // Report availability metrics
      long sourceToDestinationLatencyMs = System.currentTimeMillis() - eventsSourceTimestamp;
      // Using a time sliding window for reporting latency specifically.
      // Otherwise we report very stuck max value for slow source
      _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, topicOrDatastreamName,
          EVENTS_LATENCY_MS_STRING, LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
      _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, AGGREGATE, EVENTS_LATENCY_MS_STRING,
          LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
      _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, _datastreamTask.getConnectorType(),
          EVENTS_LATENCY_MS_STRING, LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);

      // Only update the per topic latency metric here if 'enablePerTopicMetrics' is false, otherwise this will
      // update the metric twice.
      if (_enablePerTopicEventLatencyMetrics && !_enablePerTopicMetrics) {
        _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, metadata.getTopic(),
            EVENTS_LATENCY_MS_STRING, LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
      }

      reportSLAMetrics(topicOrDatastreamName, sourceToDestinationLatencyMs <= _availabilityThresholdSlaMs,
          EVENTS_PRODUCED_WITHIN_SLA, EVENTS_PRODUCED_OUTSIDE_SLA);

      reportSLAMetrics(topicOrDatastreamName, sourceToDestinationLatencyMs <= _availabilityThresholdAlternateSlaMs,
          EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA, EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA);

      if (_logger.isDebugEnabled()) {
        if (sourceToDestinationLatencyMs > _availabilityThresholdSlaMs) {
          _logger.debug(
              "Event latency of {} for source {}, datastream {}, topic {}, partition {} exceeded SLA of {} milliseconds",
              sourceToDestinationLatencyMs, _datastreamTask.getDatastreamSource().getConnectionString(), datastreamName,
              metadata.getTopic(), metadata.getPartition(), _availabilityThresholdSlaMs);
        }
        if (sourceToDestinationLatencyMs > _availabilityThresholdAlternateSlaMs) {
          _logger.debug(
              "Event latency of {} for source {}, datastream {}, topic {}, partition {} exceeded SLA of {} milliseconds",
              sourceToDestinationLatencyMs, _datastreamTask.getDatastreamSource().getConnectionString(), datastreamName,
              metadata.getTopic(), metadata.getPartition(), _availabilityThresholdAlternateSlaMs);
        }
      }

      _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, TOTAL_EVENTS_PRODUCED, 1);
      _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(), TOTAL_EVENTS_PRODUCED,
          1);

      // Log information about events if either warn logging is enabled or logging for topic partitions outside
      // alternate SLA is enabled
      performSlaRelatedLogging(metadata, eventsSourceTimestamp, sourceToDestinationLatencyMs);
    }

    // Report the time it took to just send the events to destination
    if (eventsSendTimestamp > 0) {
      long sendLatency = System.currentTimeMillis() - eventsSendTimestamp;
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, topicOrDatastreamName, EVENTS_SEND_LATENCY_MS_STRING,
          sendLatency);
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, AGGREGATE, EVENTS_SEND_LATENCY_MS_STRING, sendLatency);
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, _datastreamTask.getConnectorType(),
          EVENTS_SEND_LATENCY_MS_STRING, sendLatency);
    }
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, AGGREGATE, EVENT_PRODUCE_RATE, 1);
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, _datastreamTask.getConnectorType(), EVENT_PRODUCE_RATE, 1);
  }

  private void onSendCallback(DatastreamRecordMetadata metadata, Exception exception, SendCallback sendCallback,
      long eventSourceTimestamp, long eventSendTimestamp) {

    SendFailedException sendFailedException = null;

    if (exception != null) {
      // If it is custom checkpointing it is up to the connector to keep track of the safe checkpoints.
      Map<Integer, String> safeCheckpoints = _checkpointProvider.getSafeCheckpoints(_datastreamTask);
      sendFailedException = new SendFailedException(_datastreamTask, safeCheckpoints, exception);
    } else {
      // Report metrics
      checkpoint(metadata.getPartition(), metadata.getCheckpoint());
      reportMetrics(metadata, eventSourceTimestamp, eventSendTimestamp);
    }

    // Inform the connector about the success or failure, In the case of failure,
    // the connector is expected retry and go back to the last checkpoint.
    if (sendCallback != null) {
      sendCallback.onCompletion(metadata, sendFailedException);
    }
  }

  /**
   * Looks for config {@value CFG_SKIP_MSG_SERIALIZATION_ERRORS} in the datastream metadata and returns its value.
   * Default value is false.
   */
  private Boolean getSkipMessageOnSerializationErrors(DatastreamTask task, Properties config) {
    // Find the producer config
    String skipMessageOnSerializationErrors =
        config.getProperty(CFG_SKIP_MSG_SERIALIZATION_ERRORS, DEFAULT_SKIP_MSG_SERIALIZATION_ERRORS);

    // Datastream configuration will override the producer config.
    return Boolean.parseBoolean(task.getDatastreams()
        .stream()
        .findFirst()
        .map(Datastream::getMetadata)
        .map(metadata -> metadata.getOrDefault(CFG_SKIP_MSG_SERIALIZATION_ERRORS, skipMessageOnSerializationErrors))
        .orElse(skipMessageOnSerializationErrors));
  }

  @Override
  public void flush() {
    Instant beforeFlush = Instant.now();
    try {
      _transportProvider.flush();
      _checkpointProvider.flush();
      _lastFlushTime = Instant.now();
    } finally {
      // Report flush latency metrics
      long flushLatencyMs = Duration.between(beforeFlush, _lastFlushTime).toMillis();
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, AGGREGATE, FLUSH_LATENCY_MS_STRING, flushLatencyMs);
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, _datastreamTask.getConnectorType(), FLUSH_LATENCY_MS_STRING,
          flushLatencyMs);

      if (flushLatencyMs > LONG_FLUSH_WARN_THRESHOLD_MS) {
        _logger.warn("Flush took {} ms", flushLatencyMs);
      }
    }
  }

  /**
   * Inform the checkpoint provider about the new safe checkpoints.
   */
  private void checkpoint(int partition, String checkpoint) {
    DatastreamTaskImpl task = (DatastreamTaskImpl) _datastreamTask;
    try {
      _checkpointProvider.updateCheckpoint(task, partition, checkpoint);
      task.updateCheckpoint(partition, checkpoint);
    } catch (Exception e) {
      String errorMessage = String.format("Checkpoint commit failed, task = [%s].", task);
      ErrorLogger.logAndThrowDatastreamRuntimeException(_logger, errorMessage, e);
    }
  }

  /**
   * Shuts down the event producer by flushing the checkpoints and closing the transport provider
   */
  public void shutdown(boolean skipCheckpoint) {
    if (!skipCheckpoint) {
      _checkpointProvider.flush();
    }
    _transportProvider.close();
  }

  @Override
  public String toString() {
    return String.format("EventProducer producerId=%d", _producerId);
  }

  private String getDatastreamName() {
    return _datastreamTask.getDatastreams().get(0).getName();
  }

  /**
   * Get the list of metrics maintained by the event producer
   */
  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();

    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_WITHIN_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + TOTAL_EVENTS_PRODUCED));
    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX + EVENT_PRODUCE_RATE));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_OUTSIDE_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + DROPPED_SENT_FROM_SERIALIZATION_ERROR));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_LATENCY_MS_STRING, Optional.of(
        Arrays.asList(BrooklinHistogramInfo.PERCENTILE_50, BrooklinHistogramInfo.PERCENTILE_99,
            BrooklinHistogramInfo.PERCENTILE_999))));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_SEND_LATENCY_MS_STRING));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + FLUSH_LATENCY_MS_STRING));

    return Collections.unmodifiableList(metrics);
  }
}
