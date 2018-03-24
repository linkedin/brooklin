package com.linkedin.datastream.server;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.Validate;
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
import com.linkedin.datastream.serde.SerDeSet;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.SendFailedException;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.providers.NoOpCheckpointProvider;


/**
 * EventProducer class uses the transport to send events and handles save/restore checkpoints
 * automatically if {@link EventProducer.CheckpointPolicy#DATASTREAM} is specified.
 * Otherwise, it exposes the safe checkpoints which are guaranteed to have been flushed.
 */
public class EventProducer implements DatastreamEventProducer {

  private final DatastreamTask _datastreamTask;
  private final SerDeSet _serDeSet;

  /**
   * Policy for checkpoint handling
   */
  enum CheckpointPolicy {
    DATASTREAM,
    CUSTOM
  }

  private static final String MODULE = EventProducer.class.getSimpleName();
  private static final String METRICS_PREFIX = MODULE + MetricsAware.KEY_REGEX;

  private static final AtomicInteger PRODUCER_ID_SEED = new AtomicInteger(0);

  public static final String CONFIG_FLUSH_INTERVAL_MS = "flushIntervalMs";

  // Default flush interval, It is intentionally kept at low frequency. If a particular connectors wants
  // a more frequent flush (high traffic connectors), it can perform that on it's own.
  public static final String DEFAULT_FLUSH_INTERVAL_MS = String.valueOf(Duration.ofMinutes(5).toMillis());

  private final int _producerId;
  private final Logger _logger;

  private final TransportProvider _transportProvider;
  private final CheckpointProvider _checkpointProvider;
  private final CheckpointPolicy _checkpointPolicy;

  private final DynamicMetricsManager _dynamicMetricsManager;
  private static final String TOTAL_EVENTS_PRODUCED = "totalEventsProduced";
  private static final String EVENTS_PRODUCED_WITHIN_SLA = "eventsProducedWithinSla";
  private static final String EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA = "eventsProducedWithinAlternateSla";
  private static final String EVENT_PRODUCE_RATE = "eventProduceRate";
  private static final String EVENTS_LATENCY_MS_STRING = "eventsLatencyMs";
  private static final String EVENTS_SEND_LATENCY_MS_STRING = "eventsSendLatencyMs";
  private static final String FLUSH_LATENCY_MS_STRING = "flushLatencyMs";

  private static final String AVAILABILITY_THRESHOLD_SLA_MS = "availabilityThresholdSlaMs";
  private static final String AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS = "availabilityThresholdAlternateSlaMs";
  private static final String EVENTS_PRODUCED_OUTSIDE_SLA = "eventsProducedOutsideSla";
  private static final String EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA = "eventsProducedOutsideAlternateSla";
  private static final String AGGREGATE = "aggregate";
  private static final String DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS = "60000"; // 1 minute
  private static final String DEFAULT_AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS = "180000"; // 3 minutes
  private static final long LATENCY_SLIDING_WINDOW_LENGTH_MS = Duration.ofMinutes(5).toMillis();

  public static final String CFG_SKIP_BAD_MESSAGE = "skipBadMessage";
  public static final String SKIPPED_BAD_MESSAGES_COUNTER = "skippedBadMessagesCounter";

  private final String _datastreamName;
  private final int _availabilityThresholdSlaMs;
  // Alternate SLA for comparision with the main
  private final int _availabilityThresholdAlternateSlaMs;
  private Instant _lastFlushTime = Instant.now();
  private final Duration _flushInterval;
  private final boolean _skipBadMessagesEnabled;

  /**
   * Construct an EventProducer instance.
   * @param transportProvider event transport
   * @param checkpointProvider checkpoint store
   * @param config global config
   * @param customCheckpointing decides whether Producer should use custom checkpointing or the datastream server
   *                            provided checkpointing.
   */
  public EventProducer(DatastreamTask task, TransportProvider transportProvider, CheckpointProvider checkpointProvider,
      Properties config, boolean customCheckpointing) {
    Validate.notNull(transportProvider, "null transport provider");
    Validate.notNull(checkpointProvider, "null checkpoint provider");
    Validate.notNull(config, "null config");

    _datastreamTask = task;
    _datastreamName = task.getDatastreams().get(0).getName();
    _serDeSet = task.getDestinationSerDes();
    _transportProvider = transportProvider;
    _checkpointPolicy = customCheckpointing ? CheckpointPolicy.CUSTOM : CheckpointPolicy.DATASTREAM;
    _producerId = PRODUCER_ID_SEED.getAndIncrement();
    _logger = LoggerFactory.getLogger(String.format("%s:%d", MODULE, _producerId));

    if (customCheckpointing) {
      _checkpointProvider = new NoOpCheckpointProvider();
    } else {
      _checkpointProvider = checkpointProvider;
    }

    _availabilityThresholdSlaMs =
        Integer.parseInt(config.getProperty(AVAILABILITY_THRESHOLD_SLA_MS, DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS));

    _availabilityThresholdAlternateSlaMs =
        Integer.parseInt(config.getProperty(AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS,
            DEFAULT_AVAILABILITY_THRESHOLD_ALTERNATE_SLA_MS));

    _flushInterval =
        Duration.ofMillis(Long.parseLong(config.getProperty(CONFIG_FLUSH_INTERVAL_MS, DEFAULT_FLUSH_INTERVAL_MS)));

    _logger.info(String.format("Created event producer with customCheckpointing=%s", customCheckpointing));

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    // provision some metrics to force them to create
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, EVENTS_PRODUCED_OUTSIDE_SLA, 0);
    _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(),
        EVENTS_PRODUCED_OUTSIDE_SLA, 0);

    _skipBadMessagesEnabled = skipBadMessageEnabled(task);
    if (_skipBadMessagesEnabled) {
      _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamName, SKIPPED_BAD_MESSAGES_COUNTER, 0);
    }
  }

  public int getProducerId() {
    return _producerId;
  }

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
   * @param record DatastreamEvent envelope
   * @param sendCallback
   */
  @Override
  public void send(DatastreamProducerRecord record, SendCallback sendCallback) {
    try {
      // Validate
      validateEventRecord(record);
      // Serialize
      record.serializeEvents(_datastreamTask.getDestinationSerDes());
      // Send
      String destination =
          record.getDestination().orElse(_datastreamTask.getDatastreamDestination().getConnectionString());
      record.setEventsSendTimestamp(System.currentTimeMillis());
      _transportProvider.send(destination, record,
          (metadata, exception) -> onSendCallback(metadata, exception, sendCallback, record));
    } catch (Exception e) {
      String errorMessage = String.format("Failed send the event %s exception %s", record, e);
      _logger.warn(errorMessage, e);
      if (_skipBadMessagesEnabled) {
      /*
       * If flag _skipBadMessagesEnabled is set, then message are skipped after an unsuccessful send
       * Example of problems:
       * - Message is above the message size supported by the destination.
       * - The message can not be encoded to conform to the destination format (e.g. missing a field).
       *
       * This flag should only be set to true for use cases that can tolerate messages lost.
       *
       * Unfortunately the error could be a transient network problem, and not a problem with the message itself.
       * For this reason is strongly recommended to put alerts in the SKIPPED_BAD_MESSAGES_RATE.
       *
       * TODO: Try to define a special exception for "badMessage" so we can differentiate between a send error,
       * or a message compliance error. Right now is very hard to do that, because  will require to refactor a lot
       * of library and code we do not control.
       */
        _logger.error("Skipping Message. task: {} ; error: {}", _datastreamTask, e);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamName, SKIPPED_BAD_MESSAGES_COUNTER, 1);
      } else {
        throw new DatastreamRuntimeException(errorMessage, e);
      }
    }

    // It is possible that the connector is not calling flush at regular intervals, In which case we will force a periodic flush.
    if (Instant.now().isAfter(_lastFlushTime.plus(_flushInterval))) {
      flush();
    }
  }

  // Report metrics on every send callback from the transport provider. Because this can be invoked multiple times
  // per DatastreamProducerRecord (i.e. by the number of events within the record), only increment all metrics by 1
  // to avoid over-counting.
  private void reportMetrics(DatastreamRecordMetadata metadata, DatastreamProducerRecord record) {
    // Treat all events within this record equally (assume same timestamp)
    if (record.getEventsSourceTimestamp() > 0) {
      // Report availability metrics
      long sourceToDestinationLatencyMs = System.currentTimeMillis() - record.getEventsSourceTimestamp();
      // Using a time sliding window for reporting latency specifically.
      // Otherwise we report very stuck max value for slow source
      _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, metadata.getTopic(), EVENTS_LATENCY_MS_STRING,
          LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
      _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, AGGREGATE, EVENTS_LATENCY_MS_STRING,
          LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);
      _dynamicMetricsManager.createOrUpdateSlidingWindowHistogram(MODULE, _datastreamTask.getConnectorType(),
          EVENTS_LATENCY_MS_STRING, LATENCY_SLIDING_WINDOW_LENGTH_MS, sourceToDestinationLatencyMs);

      if (sourceToDestinationLatencyMs <= _availabilityThresholdSlaMs) {
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, EVENTS_PRODUCED_WITHIN_SLA, 1);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(),
            EVENTS_PRODUCED_WITHIN_SLA, 1);
      } else {
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, metadata.getTopic(), EVENTS_PRODUCED_OUTSIDE_SLA, 1);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, EVENTS_PRODUCED_OUTSIDE_SLA, 1);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(),
            EVENTS_PRODUCED_OUTSIDE_SLA, 1);
        _logger.debug(
            String.format("Event latency of %d for source %s, topic %s, partition %d exceeded SLA of %d milliseconds",
                sourceToDestinationLatencyMs, _datastreamTask.getDatastreamSource().getConnectionString(),
                metadata.getTopic(), metadata.getPartition(), _availabilityThresholdSlaMs));
      }

      if (sourceToDestinationLatencyMs <= _availabilityThresholdAlternateSlaMs) {
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA, 1);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(),
            EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA, 1);
      } else {
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, metadata.getTopic(), EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA, 1);
        _logger.debug(
            String.format("Event latency of %d for source %s, topic %s, partition %d exceeded SLA of %d milliseconds",
                sourceToDestinationLatencyMs, _datastreamTask.getDatastreamSource().getConnectionString(),
                metadata.getTopic(), metadata.getPartition(), _availabilityThresholdAlternateSlaMs));
      }

      _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, TOTAL_EVENTS_PRODUCED, 1);
      _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(), TOTAL_EVENTS_PRODUCED,
          1);
    }

    // Report the time it took to just send the events to destination
    record.getEventsSendTimestamp().ifPresent(sendTimestamp -> {
      long sendLatency = System.currentTimeMillis() - sendTimestamp;
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, metadata.getTopic(), EVENTS_SEND_LATENCY_MS_STRING,
          sendLatency);
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, AGGREGATE, EVENTS_SEND_LATENCY_MS_STRING, sendLatency);
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, _datastreamTask.getConnectorType(),
          EVENTS_SEND_LATENCY_MS_STRING, sendLatency);
    });

    _dynamicMetricsManager.createOrUpdateMeter(MODULE, AGGREGATE, EVENT_PRODUCE_RATE, 1);
    _dynamicMetricsManager.createOrUpdateMeter(MODULE, _datastreamTask.getConnectorType(), EVENT_PRODUCE_RATE, 1);
  }

  private void onSendCallback(DatastreamRecordMetadata metadata, Exception exception, SendCallback sendCallback,
      DatastreamProducerRecord record) {

    SendFailedException sendFailedException = null;

    if (exception != null) {
      // If it is custom checkpointing it is upto the connector to keep track of the safe checkpoints.
      Map<Integer, String> safeCheckpoints = _checkpointProvider.getSafeCheckpoints(_datastreamTask);
      sendFailedException = new SendFailedException(_datastreamTask, safeCheckpoints, exception);
    } else {
      // Report metrics
      checkpoint(metadata.getPartition(), metadata.getCheckpoint());
      reportMetrics(metadata, record);
    }

    // Inform the connector about the success or failure, In the case of failure,
    // the connector is expected retry and go back to the last checkpoint.
    if (sendCallback != null) {
      sendCallback.onCompletion(metadata, sendFailedException);
    }
  }

  @Override
  public void flush() {
    Instant beforeFlush = Instant.now();
    _transportProvider.flush();
    _checkpointProvider.flush();
    _lastFlushTime = Instant.now();

    // Report flush latency metrics
    long flushLatencyMs = Duration.between(beforeFlush, _lastFlushTime).toMillis();
    _dynamicMetricsManager.createOrUpdateHistogram(MODULE, AGGREGATE, FLUSH_LATENCY_MS_STRING, flushLatencyMs);
    _dynamicMetricsManager.createOrUpdateHistogram(MODULE, _datastreamTask.getConnectorType(), FLUSH_LATENCY_MS_STRING,
        flushLatencyMs);
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

  public void shutdown() {
    _checkpointProvider.flush();
    _transportProvider.close();
  }

  @Override
  public String toString() {
    return String.format("EventProducer producerId=%d", _producerId);
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    String className = EventProducer.class.getSimpleName();

    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_WITHIN_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_WITHIN_ALTERNATE_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + TOTAL_EVENTS_PRODUCED));
    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX + EVENT_PRODUCE_RATE));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + SKIPPED_BAD_MESSAGES_COUNTER));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_OUTSIDE_SLA));
    metrics.add(new BrooklinCounterInfo(METRICS_PREFIX + EVENTS_PRODUCED_OUTSIDE_ALTERNATE_SLA));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_LATENCY_MS_STRING, Optional.of(
        Arrays.asList(BrooklinHistogramInfo.MEAN, BrooklinHistogramInfo.MAX, BrooklinHistogramInfo.PERCENTILE_50,
            BrooklinHistogramInfo.PERCENTILE_99, BrooklinHistogramInfo.PERCENTILE_999))));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + EVENTS_SEND_LATENCY_MS_STRING));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX + FLUSH_LATENCY_MS_STRING));

    return Collections.unmodifiableList(metrics);
  }

  /**
   * Look for config {@value CFG_SKIP_BAD_MESSAGE} in the datastream metadata and returns its value.
   * Default value is false.
   */
  private static boolean skipBadMessageEnabled(DatastreamTask task) {
    return task.getDatastreams()
        .stream()
        .findAny()
        .map(Datastream::getMetadata)
        .map(metadata -> metadata.getOrDefault(CFG_SKIP_BAD_MESSAGE, "false").toLowerCase().equals("true"))
        .orElse(false);
  }
}
