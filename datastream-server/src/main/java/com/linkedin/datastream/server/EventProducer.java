/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
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
  public static final String CFG_DISABLE_SLA_METRIC = "system.disableSlaMetric";
  public static final String CONFIG_FLUSH_INTERVAL_MS = "flushIntervalMs";
  public static final String CONFIG_ENABLE_PER_TOPIC_METRICS = "enablePerTopicMetrics";
  public static final String CONFIG_ENABLE_PER_TOPIC_EVENT_LATENCY_METRICS = "enablePerTopicEventLatencyMetrics";
  /**
   * When enabled, emits per-source-database throughput attribution metrics keyed as
   * {@code EventProducer.db.<databaseName>.bytesProducedRate} and
   * {@code EventProducer.db.<databaseName>.eventProduceRate}.
   * Applies only to CDC connectors whose source URI uses a single-slash scheme
   * (e.g. {@code espresso:/}, {@code mysql:/}, {@code tidb:/}).
   * Double-slash URIs (e.g. {@code kafka://}) produce no database metrics.
   *
   * <p><b>Cardinality warning:</b> each distinct database name creates a new metric series.
   * Enable only when the set of source databases is bounded and well-understood.
   */
  public static final String CONFIG_ENABLE_THROUGHPUT_METRICS = "enableThroughputAttributionMetrics";

  // Default flush interval, It is intentionally kept at low frequency. If a particular connectors wants
  // a more frequent flush (high traffic connectors), it can perform that on it's own.
  public static final String DEFAULT_FLUSH_INTERVAL_MS = String.valueOf(Duration.ofMinutes(5).toMillis());

  static final String EVENTS_LATENCY_MS_STRING = "eventsLatencyMs";
  static final String EVENTS_LATENCY_MS_SLA_INELIGIBLE_STRING = "eventsLatencyMsSlaIneligible";
  static final String EVENTS_SEND_LATENCY_MS_STRING = "eventsSendLatencyMs";
  // Source DB commit timestamp -> destination ack latency. Distinct from eventsLatencyMs, which on Espresso/TiDB
  // measures from the intermediate Kafka append time rather than the original DB commit.
  static final String EVENTS_COMMIT_TO_ACK_LATENCY_MS_STRING = "eventsCommitToAckLatencyMs";
  static final String EVENTS_COMMIT_TO_ACK_LATENCY_MS_SLA_INELIGIBLE_STRING = "eventsCommitToAckLatencyMsSlaIneligible";
  static final String THROUGHPUT_VIOLATING_EVENTS_LATENCY_MS_STRING = "throughputViolatingEventsLatencyMs";
  static final String THROUGHPUT_VIOLATING_EVENTS_SEND_LATENCY_MS_STRING = "throughputViolatingEventsSendLatencyMs";

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
  private static final String NEW_STREAM_GRACE_PERIOD_MS = "newStreamGracePeriodMs";
  private static final String DEFAULT_NEW_STREAM_GRACE_PERIOD_MS = "0";
  private static final String EVENTS_PRODUCED_OUTSIDE_SLA = "eventsProducedOutsideSla";
  private static final String EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA = "eventsProducedOutsideAlternateSla";
  private static final String EVENTS_COMMIT_WITHIN_SLA = "eventsCommitWithinSla";
  private static final String EVENTS_COMMIT_OUTSIDE_SLA = "eventsCommitOutsideSla";
  private static final String EVENTS_COMMIT_WITHIN_ALTERNATE_SLA = "eventsCommitWithinAlternateSla";
  private static final String EVENTS_COMMIT_OUTSIDE_ALTERNATE_SLA = "eventsCommitOutsideAlternateSla";
  private static final String COMMIT_TO_ACK_THRESHOLD_SLA_MS = "commitToAckThresholdSlaMs";
  private static final String COMMIT_TO_ACK_THRESHOLD_ALTERNATE_SLA_MS = "commitToAckThresholdAlternateSlaMs";
  private static final String DROPPED_SENT_FROM_SERIALIZATION_ERROR = "droppedSentFromSerializationError";
  static final String BYTES_PRODUCED_RATE = "bytesProducedRate";
  private static final String AGGREGATE = "aggregate";

  private static final String DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS = "60000"; // 1 minute
  private static final String DEFAULT_AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS = "180000"; // 3 minutes
  // Commit-to-ack covers upstream CDC pipeline lag in addition to Brooklin-side transport, so defaults are wider
  // than the source-to-ack thresholds above.
  private static final String DEFAULT_COMMIT_TO_ACK_THRESHOLD_SLA_MS = "300000"; // 5 minutes
  private static final String DEFAULT_COMMIT_TO_ACK_THRESHOLD_ALTERNATE_SLA_MS = "900000"; // 15 minutes
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
  // Commit-to-ack (DB commit time -> destination ack) SLA thresholds. Used only when the connector supplies a
  // commit timestamp on the producer record (CDC connectors).
  private final int _commitToAckThresholdSlaMs;
  private final int _commitToAckThresholdAlternateSlaMs;
  // Grace period for newly created streams. While a stream is inside this window, primary/alternate
  // SLA counters are suppressed and the latency histogram is redirected to eventsLatencyMsSlaIneligible.
  private final long _newStreamGracePeriodMs;
  // Timestamp when the stream was created (from datastream metadata)
  private final long _streamCreationTimeMs;
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
  private final boolean _disableSlaMetric;
  private final boolean _enableThroughputMetrics;
  // Cached source database name parsed from the connection string at construction time (null for non-CDC sources)
  private final String _sourceDatabase;
  private final Duration _flushInterval;
  private final Function<DatastreamTask, Set<String>> _throughputViolatingTopicsProvider;

  private Instant _lastFlushTime = Instant.now();
  private long _lastEventsOutsideAltSlaLogTimeMs = System.currentTimeMillis();
  private Map<TopicPartition, Integer> _trackEventsOutsideAltSlaMap = new ConcurrentHashMap<>();
  private boolean _enableFlushOnSend = true;

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
    this(task, transportProvider, checkpointProvider, config, customCheckpointing, (t) -> Collections.emptySet());
  }

  /**
   * Construct an EventProducer instance.
   * @param transportProvider the transport provider
   * @param checkpointProvider the checkpoint provider
   * @param config the config options
   * @param customCheckpointing decides whether producer should use custom checkpointing or the datastream server
   *                            provided checkpointing.
   * @param throughputViolatingTopicsProvider function parameter per task to find the throughput violating topics
   */
  public EventProducer(DatastreamTask task, TransportProvider transportProvider, CheckpointProvider checkpointProvider,
      Properties config, boolean customCheckpointing,
      Function<DatastreamTask, Set<String>> throughputViolatingTopicsProvider) {
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(config, "null config");

    _datastreamTask = task;
    _transportProvider = transportProvider;
    _producerId = PRODUCER_ID_SEED.getAndIncrement();
    _logger = LoggerFactory.getLogger(String.format("%s:%d", MODULE, _producerId));
    _throughputViolatingTopicsProvider = throughputViolatingTopicsProvider;

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

    _commitToAckThresholdSlaMs = Integer.parseInt(
        config.getProperty(COMMIT_TO_ACK_THRESHOLD_SLA_MS, DEFAULT_COMMIT_TO_ACK_THRESHOLD_SLA_MS));
    _commitToAckThresholdAlternateSlaMs = Integer.parseInt(
        config.getProperty(COMMIT_TO_ACK_THRESHOLD_ALTERNATE_SLA_MS, DEFAULT_COMMIT_TO_ACK_THRESHOLD_ALTERNATE_SLA_MS));

    _newStreamGracePeriodMs = Long.parseLong(
        config.getProperty(NEW_STREAM_GRACE_PERIOD_MS, DEFAULT_NEW_STREAM_GRACE_PERIOD_MS));
    _streamCreationTimeMs = parseStreamCreationTimeMs(task);

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

    _disableSlaMetric = getDisableSlaMetric(task);

    _enableThroughputMetrics =
        Boolean.parseBoolean(config.getProperty(CONFIG_ENABLE_THROUGHPUT_METRICS, Boolean.FALSE.toString()));

    String[] sourceParts = getSourcePathParts();
    _sourceDatabase = (sourceParts != null && sourceParts.length > 1) ? sourceParts[1] : null;

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

  @Override
  public DatastreamRecordMetadata broadcast(DatastreamProducerRecord record, SendCallback sendEventCallback) {
    return helperSendOrBroadcast(record, sendEventCallback, true);
  }

  @Override
  public void send(DatastreamProducerRecord record, SendCallback sendCallback) {
    helperSendOrBroadcast(record, sendCallback, false);
  }

  /**
   * Send the event onto the underlying transport.
   * @param record the datastream event
   * @param sendEventCallback the callback to be invoked after the event is sent to the destination
   *
   * @return For broadcast return DatastreamRecordMetadata got from transport provider broadcast, null for send
   */
  private DatastreamRecordMetadata helperSendOrBroadcast(DatastreamProducerRecord record,
      SendCallback sendEventCallback, boolean isBroadcast) {
    DatastreamRecordMetadata broadcastMetadata = null;

    try {
      validateEventRecord(record);

      record.serializeEvents(_datastreamTask.getDestinationSerDes());
    } catch (NullPointerException e) {
      String errorMessage = String.format("Validation failed for record %s exception %s", record, e);
      _logger.warn(errorMessage, e);
      throw new DatastreamRuntimeException(errorMessage, e);
    } catch (Exception e) {
      if (_skipMessageOnSerializationErrors) {
        _logger.info(String.format("Skipping the message on serialization error as configured. "
                + "Datastream name: %s, Datastream task name: %s",
            getDatastreamName(), _datastreamTask.getDatastreamTaskName()), e);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, getDatastreamName(),
            DROPPED_SENT_FROM_SERIALIZATION_ERROR, 1);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, DROPPED_SENT_FROM_SERIALIZATION_ERROR, 1);
        if (isBroadcast) {
          return new DatastreamRecordMetadata(true);
        }
        return null;
      } else {
        String errorMessage = String.format("Failed to send event %s because of serialization exception %s", record, e);
        _logger.warn(errorMessage, e);
        throw new DatastreamRuntimeException(errorMessage, e);
      }
    }

    try {
      // Send the event to the transport
      String destination = StringUtils.EMPTY;
      if (!_datastreamTask.getTransportProviderName().
          equalsIgnoreCase(NoOpTransportProviderAdminFactory.NoOpTransportProvider.NAME)) {
        destination =
            record.getDestination().orElse(_datastreamTask.getDatastreamDestination().getConnectionString());
      }
      record.setEventsSendTimestamp(System.currentTimeMillis());
      long recordEventsSourceTimestamp = record.getEventsSourceTimestamp();
      long recordEventsSendTimestamp = record.getEventsSendTimestamp().orElse(0L);
      // Absent for non-CDC connectors and for CDC bootstrap/heartbeat paths; commit-to-ack metric is skipped when absent.
      Optional<Long> recordEventsCommitTimestamp = record.getEventsCommitTimestamp();
      final long numSerializedBytes = record.getEvents().stream()
          .mapToLong(e -> {
            long keySize = e.key().filter(k -> k instanceof byte[]).map(k -> (long) ((byte[]) k).length).orElse(0L);
            long valSize = e.value().filter(v -> v instanceof byte[]).map(v -> (long) ((byte[]) v).length).orElse(0L);
            return keySize + valSize;
          })
          .sum();
      if (isBroadcast) {
        broadcastMetadata = _transportProvider.broadcast(destination, record,
            (metadata, exception) -> onSendCallback(metadata, exception, sendEventCallback, recordEventsSourceTimestamp,
                recordEventsSendTimestamp, numSerializedBytes, recordEventsCommitTimestamp));
        _logger.debug("Broadcast completed with {}", broadcastMetadata);
        if (broadcastMetadata.isMessageSerializationError()) {
          _logger.warn("Broadcast of record {} to destination {} failed because of serialization error.",
              record, destination);
        }
      } else {
        _transportProvider.send(destination, record,
            (metadata, exception) -> onSendCallback(metadata, exception, sendEventCallback, recordEventsSourceTimestamp,
                recordEventsSendTimestamp, numSerializedBytes, recordEventsCommitTimestamp));
      }
    } catch (Exception e) {
      String errorMessage = String.format("Failed to send the event %s exception %s", record, e);
      _logger.warn(errorMessage, e);
      throw new DatastreamRuntimeException(errorMessage, e);
    }

    // Force a periodic flush if flushless mode isn't enabled, in case the connector is not calling flush at
    // regular intervals
    if (_enableFlushOnSend && Instant.now().isAfter(_lastFlushTime.plus(_flushInterval))) {
      flush();
    }

    return broadcastMetadata;
  }

  private boolean isCdcSource() {
    return _sourceDatabase != null;
  }

  /**
   * Returns true while a CDC stream is still within its cdc-catch-up grace window. Non-CDC
   * sources (BMM kafka://, Inlogs, etc.) always return false here so neither SLA suppression
   * nor the latency-histogram redirect applies to them.
   */
  private boolean isWithinGracePeriod() {
    if (!isCdcSource()) {
      return false;
    }
    return (System.currentTimeMillis() - _streamCreationTimeMs) < _newStreamGracePeriodMs;
  }

  /**
   * Single gate that decides whether to emit metrics to their regular destinations. When this
   * returns false, primary and alternate SLA counters are suppressed and the latency histogram
   * is redirected to eventsLatencyMsSlaIneligible. Combines all suppression conditions in one place
   * — additional conditions (e.g. per-datastream opt-out flags) should be ORed in here so call
   * sites do not need to know about every gating condition individually.
   */
  private boolean shouldEmitMetric() {
    if (_disableSlaMetric) {
      return false;
    }
    if (isWithinGracePeriod()) {
      return false;
    }
    return true;
  }

  /**
   * Parse the stream creation time used for SLA grace-period gating. When multiple datastreams
   * share a task via dedup, returns the oldest (min) CREATION_MS so a freshly deduped stream
   * cannot suppress SLA on long-running siblings. Zero / missing / malformed values are filtered
   * out; the method returns 0 in those cases (grace disabled, fail-open to SLA reporting).
   */
  private long parseStreamCreationTimeMs(DatastreamTask task) {
    try {
      if (task.getDatastreams() != null && !task.getDatastreams().isEmpty()) {
        return task.getDatastreams().stream()
            .map(ds -> ds.getMetadata().getOrDefault(DatastreamMetadataConstants.CREATION_MS, "0"))
            .mapToLong(Long::parseLong)
            .filter(ms -> ms > 0)
            .min()
            .orElse(0L);
      }
    } catch (Exception e) {
      _logger.warn("Failed to parse stream creation time, SLA grace period will not be applied", e);
    }
    return 0L;
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
      try {
        if (sourceToDestinationLatencyMs > _availabilityThresholdAlternateSlaMs) {
          TopicPartition topicPartition = new TopicPartition(metadata.getUndecoratedTopic(), metadata.getSourcePartition());
          int numEvents = _trackEventsOutsideAltSlaMap.getOrDefault(topicPartition, 0);
          _trackEventsOutsideAltSlaMap.put(topicPartition, numEvents + 1);
        }

        long timeSinceLastLog = System.currentTimeMillis() - _lastEventsOutsideAltSlaLogTimeMs;
        if (timeSinceLastLog >= _numEventsOutsideAltSlaFrequencyMs) {
          _trackEventsOutsideAltSlaMap.forEach((topicPartition, numEvents) -> _logger.warn(
              "{} had {} event(s) with latency greater than alternate SLA of {} ms in the last {} ms for "
                  + "datastream {}", topicPartition, numEvents, _availabilityThresholdAlternateSlaMs, timeSinceLastLog,
              getDatastreamName()));
          _trackEventsOutsideAltSlaMap.clear();
          _lastEventsOutsideAltSlaLogTimeMs = System.currentTimeMillis();
        }
      } catch (NullPointerException | IllegalArgumentException | ClassCastException | UnsupportedOperationException e) {
        // Catch any exceptions that can be thrown for HashMap operations to avoid being in a situation where the
        // send callback is unable to complete. Don't catch all exceptions as certain exceptions should be propagated
        // up such as out of memory errors.
        _logger.warn("Could not perform warn logging due to exception: ", e);
      }
    }
  }

  /**
   * Report metrics on every send callback from the transport provider. Because this can be invoked multiple times
   * per DatastreamProducerRecord (i.e. by the number of events within the record), only increment all metrics by 1
   * to avoid overcounting.
   */
  private void reportMetrics(DatastreamRecordMetadata metadata, long eventsSourceTimestamp, long eventsSendTimestamp,
      long numBytes, Optional<Long> eventsCommitTimestamp) {
    reportCommitToAckMetrics(metadata, eventsCommitTimestamp);
    // If per-topic metrics are enabled, use topic as key for metrics; else, use datastream name as the key
    String datastreamName = getDatastreamName();

    String topicOrDatastreamName = _enablePerTopicMetrics ? metadata.getTopic() : datastreamName;
    // Treat all events within this record equally (assume same timestamp)
    if (eventsSourceTimestamp > 0) {
      // Report availability metrics. Streams that opt out via system.disableSlaMetric still emit
      // a latency histogram, but under eventsLatencyMsSlaIneligible so they don't pollute the SLA metric.
      long sourceToDestinationLatencyMs = System.currentTimeMillis() - eventsSourceTimestamp;
      // Redirect the latency histogram to eventsLatencyMsSlaIneligible while SLA emission is suppressed
      // so lag alerts wired to eventsLatencyMs do not fire on the initial CDC catch-up. The
      // catch-up curve is still observable on eventsLatencyMsSlaIneligible.
      String latencyMetricName = shouldEmitMetric() ? EVENTS_LATENCY_MS_STRING : EVENTS_LATENCY_MS_SLA_INELIGIBLE_STRING;
      reportEventLatencyMetrics(topicOrDatastreamName, metadata, sourceToDestinationLatencyMs, latencyMetricName);

      // While shouldEmitMetric() returns false (currently only during the CDC catch-up grace
      // window) both primary and alternate SLA counter pairs are suppressed entirely.
      if (shouldEmitMetric()) {
        reportSLAMetrics(topicOrDatastreamName, sourceToDestinationLatencyMs <= _availabilityThresholdSlaMs,
            EVENTS_PRODUCED_WITHIN_SLA, EVENTS_PRODUCED_OUTSIDE_SLA);

        reportSLAMetrics(topicOrDatastreamName, sourceToDestinationLatencyMs <= _availabilityThresholdAlternateSlaMs,
            EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA, EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA);
      }

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
      reportSendLatencyMetrics(metadata, sendLatency, EVENTS_SEND_LATENCY_MS_STRING);
    }
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, AGGREGATE, EVENT_PRODUCE_RATE, 1);
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, _datastreamTask.getConnectorType(), EVENT_PRODUCE_RATE, 1);
    reportThroughputAttributionMetrics(numBytes);
  }

  /**
   * Emit commit-to-ack (DB commit -> destination ack) latency histogram and SLA counters. No-op when the connector
   * did not supply a commit timestamp (non-CDC sources, or CDC bootstrap/heartbeat paths). Gated by the same
   * shouldEmitMetric() suppression as the existing source-to-ack SLA so the new metric inherits grace-period and
   * per-datastream opt-out behavior.
   */
  private void reportCommitToAckMetrics(DatastreamRecordMetadata metadata, Optional<Long> eventsCommitTimestamp) {
    if (!eventsCommitTimestamp.isPresent()) {
      return;
    }
    long commitTs = eventsCommitTimestamp.get();
    if (commitTs <= 0) {
      return;
    }
    long commitToAckLatencyMs = System.currentTimeMillis() - commitTs;
    String topicOrDatastreamName = _enablePerTopicMetrics ? metadata.getTopic() : getDatastreamName();
    String latencyMetricName = shouldEmitMetric()
        ? EVENTS_COMMIT_TO_ACK_LATENCY_MS_STRING
        : EVENTS_COMMIT_TO_ACK_LATENCY_MS_SLA_INELIGIBLE_STRING;
    reportEventLatencyMetrics(topicOrDatastreamName, metadata, commitToAckLatencyMs, latencyMetricName);

    if (shouldEmitMetric()) {
      reportSLAMetrics(topicOrDatastreamName, commitToAckLatencyMs <= _commitToAckThresholdSlaMs,
          EVENTS_COMMIT_WITHIN_SLA, EVENTS_COMMIT_OUTSIDE_SLA);
      reportSLAMetrics(topicOrDatastreamName, commitToAckLatencyMs <= _commitToAckThresholdAlternateSlaMs,
          EVENTS_COMMIT_WITHIN_ALTERNATE_SLA, EVENTS_COMMIT_OUTSIDE_ALTERNATE_SLA);
    }
  }

  /**
   * Only for the throughput violating topics!
   * <br>
   * <br>
   * Report metrics on every send callback from the transport provider. Because this can be invoked multiple times
   * per DatastreamProducerRecord (i.e. by the number of events within the record), only increment all metrics by 1
   * to avoid overcounting.
   */
  private void reportMetricsForThroughputViolatingTopics(DatastreamRecordMetadata metadata, long eventsSourceTimestamp,
      long eventsSendTimestamp, long numBytes) {
    String topicOrDatastreamName = _enablePerTopicMetrics ? metadata.getTopic() : getDatastreamName();
    // Treat all events within this record equally (assume same timestamp)
    if (eventsSourceTimestamp > 0) {
      // Report availability metrics
      long sourceToDestinationLatencyMs = System.currentTimeMillis() - eventsSourceTimestamp;
      reportEventLatencyMetrics(topicOrDatastreamName, metadata, sourceToDestinationLatencyMs, THROUGHPUT_VIOLATING_EVENTS_LATENCY_MS_STRING);

      if (_logger.isDebugEnabled()) {
        if (sourceToDestinationLatencyMs > _availabilityThresholdAlternateSlaMs) {
          _logger.debug(
              "Event latency of {} for source {}, datastream {}, topic {}, partition {} exceeded SLA of {} milliseconds",
              sourceToDestinationLatencyMs, _datastreamTask.getDatastreamSource().getConnectionString(), getDatastreamName(),
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
      reportSendLatencyMetrics(metadata, sendLatency, THROUGHPUT_VIOLATING_EVENTS_SEND_LATENCY_MS_STRING);
    }
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, AGGREGATE, EVENT_PRODUCE_RATE, 1);
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, _datastreamTask.getConnectorType(), EVENT_PRODUCE_RATE, 1);
    reportThroughputAttributionMetrics(numBytes);
  }

  // Report Event Latency metrics for aggregate, connector and topic/datastream
  private void reportEventLatencyMetrics(String topicOrDatastreamName, DatastreamRecordMetadata metadata,
      long sourceToDestinationLatencyMs, String eventLatencyMetricName) {
    // Using a time sliding window for reporting latency specifically.
    // Otherwise we report very stuck max value for slow source
    _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, topicOrDatastreamName, eventLatencyMetricName,
        LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
    _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, AGGREGATE, eventLatencyMetricName,
        LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
    _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, _datastreamTask.getConnectorType(),
        eventLatencyMetricName, LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);

    // Only update the per topic latency metric here if 'enablePerTopicMetrics' is false, otherwise this will
    // update the metric twice.
    if (_enablePerTopicEventLatencyMetrics && !_enablePerTopicMetrics) {
      _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, metadata.getTopic(), eventLatencyMetricName,
          LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
    }
  }

  // Report Send to destination Latency metrics for aggregate, connector and topic/datastream
  private void reportSendLatencyMetrics(DatastreamRecordMetadata metadata, long sendLatency,
      String sendLatencyMetricName) {
    String topicOrDatastreamName = _enablePerTopicMetrics ? metadata.getTopic() : getDatastreamName();
    _dynamicMetricsManager.createOrUpdateHistogram(MODULE, topicOrDatastreamName,
        sendLatencyMetricName, sendLatency);
    _dynamicMetricsManager.createOrUpdateHistogram(MODULE, AGGREGATE,
        sendLatencyMetricName, sendLatency);
    _dynamicMetricsManager.createOrUpdateHistogram(MODULE, _datastreamTask.getConnectorType(),
        sendLatencyMetricName, sendLatency);
  }

  private void onSendCallback(DatastreamRecordMetadata metadata, Exception exception, SendCallback sendCallback,
      long eventSourceTimestamp, long eventSendTimestamp, long numBytes, Optional<Long> eventCommitTimestamp) {

    SendFailedException sendFailedException = null;

    try {
      if (exception != null) {
        sendFailedException = createSendFailedException(exception);
      } else {
        // Report metrics
        checkpoint(metadata.getPartition(), metadata.getCheckpoint());
        // Reporting separate metrics for throughput violating topics.

        if (_throughputViolatingTopicsProvider.apply(_datastreamTask).contains(metadata.getUndecoratedTopic())) {
          reportMetricsForThroughputViolatingTopics(metadata, eventSourceTimestamp, eventSendTimestamp, numBytes);
        } else {
          reportMetrics(metadata, eventSourceTimestamp, eventSendTimestamp, numBytes, eventCommitTimestamp);
        }
      }
    } catch (Exception e) {
      // Propagate the exception caught to the caller as a send callback exception to take any action such as retries.
      sendFailedException = sendFailedException == null ? createSendFailedException(e) : sendFailedException;
      throw e;
    } finally {
      // Inform the connector about the success or failure, In the case of failure,
      // the connector is expected retry and go back to the last checkpoint.
      if (sendCallback != null) {
        sendCallback.onCompletion(metadata, sendFailedException);
      }
    }
  }

  private SendFailedException createSendFailedException(Exception exception) {
    // If it is custom checkpointing it is up to the connector to keep track of the safe checkpoints.
    Map<Integer, String> safeCheckpoints = _checkpointProvider.getSafeCheckpoints(_datastreamTask);
    return new SendFailedException(_datastreamTask, safeCheckpoints, exception);
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

  /**
   * Looks for config {@value CFG_DISABLE_SLA_METRIC} in the datastream metadata and returns its value.
   * Default value is false.
   *
   * <p>Only honored for tasks owning a single datastream. Deduped tasks (multiple datastreams
   * sharing one EventProducer) always return false, since one stream's opt-out would otherwise
   * suppress {@code eventsLatencyMs} for every stream in the group.
   */
  private boolean getDisableSlaMetric(DatastreamTask task) {
    if (task.getDatastreams().size() > 1) {
      return false;
    }
    return Boolean.parseBoolean(task.getDatastreams()
        .stream()
        .findFirst()
        .map(Datastream::getMetadata)
        .map(metadata -> metadata.getOrDefault(CFG_DISABLE_SLA_METRIC, Boolean.FALSE.toString()))
        .orElse(Boolean.FALSE.toString()));
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

  @Override
  public void enablePeriodicFlushOnSend(boolean enableFlushOnSend) {
    _enableFlushOnSend = enableFlushOnSend;
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

  private void reportThroughputAttributionMetrics(long numBytes) {
    if (!_enableThroughputMetrics) {
      return;
    }
    if (_sourceDatabase != null) {
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "db." + _sourceDatabase, BYTES_PRODUCED_RATE, numBytes);
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, "db." + _sourceDatabase, EVENT_PRODUCE_RATE, 1);
    }
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, AGGREGATE, BYTES_PRODUCED_RATE, numBytes);
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, _datastreamTask.getConnectorType(), BYTES_PRODUCED_RATE, numBytes);
  }

  private String getDatastreamName() {
    return _datastreamTask.getDatastreams().get(0).getName();
  }

  // Returns path segments ["CLUSTER", "DATABASE", "TABLE"] for CDC single-slash URIs, null for BMM double-slash URIs.
  // Consistent with MySqlKafkaSource, TiDBKafkaSource, and EspressoSource parsing in brooklin-li-common.
  private String[] getSourcePathParts() {
    try {
      URI uri = new URI(_datastreamTask.getDatastreamSource().getConnectionString());
      if (uri.getAuthority() != null) {
        return null; // double-slash URI (e.g. kafka://host/topic) — no cluster/database segments
      }
      return uri.getPath().substring(1).split("/");
    } catch (URISyntaxException e) {
      return null;
    }
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
    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX + BYTES_PRODUCED_RATE));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_OUTSIDE_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + DROPPED_SENT_FROM_SERIALIZATION_ERROR));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_LATENCY_MS_STRING, Optional.of(
        Arrays.asList(BrooklinHistogramInfo.PERCENTILE_50, BrooklinHistogramInfo.PERCENTILE_95,
            BrooklinHistogramInfo.PERCENTILE_99, BrooklinHistogramInfo.PERCENTILE_999))));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_LATENCY_MS_SLA_INELIGIBLE_STRING, Optional.of(
        Arrays.asList(BrooklinHistogramInfo.PERCENTILE_50, BrooklinHistogramInfo.PERCENTILE_95,
            BrooklinHistogramInfo.PERCENTILE_99, BrooklinHistogramInfo.PERCENTILE_999))));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_COMMIT_TO_ACK_LATENCY_MS_STRING, Optional.of(
        Arrays.asList(BrooklinHistogramInfo.PERCENTILE_50, BrooklinHistogramInfo.PERCENTILE_95,
            BrooklinHistogramInfo.PERCENTILE_99, BrooklinHistogramInfo.PERCENTILE_999))));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_COMMIT_TO_ACK_LATENCY_MS_SLA_INELIGIBLE_STRING, Optional.of(
        Arrays.asList(BrooklinHistogramInfo.PERCENTILE_50, BrooklinHistogramInfo.PERCENTILE_95,
            BrooklinHistogramInfo.PERCENTILE_99, BrooklinHistogramInfo.PERCENTILE_999))));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_SEND_LATENCY_MS_STRING));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + THROUGHPUT_VIOLATING_EVENTS_LATENCY_MS_STRING, Optional.of(
        Arrays.asList(BrooklinHistogramInfo.PERCENTILE_50, BrooklinHistogramInfo.PERCENTILE_95,
            BrooklinHistogramInfo.PERCENTILE_99, BrooklinHistogramInfo.PERCENTILE_999))));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + THROUGHPUT_VIOLATING_EVENTS_SEND_LATENCY_MS_STRING));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + FLUSH_LATENCY_MS_STRING));

    return Collections.unmodifiableList(metrics);
  }
}
