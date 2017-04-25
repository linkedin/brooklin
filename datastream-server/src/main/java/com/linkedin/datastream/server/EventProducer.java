package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
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

  private static final AtomicInteger PRODUCER_ID_SEED = new AtomicInteger(0);

  private final int _producerId;
  private final Logger _logger;

  private final TransportProvider _transportProvider;
  private final CheckpointProvider _checkpointProvider;
  private final CheckpointPolicy _checkpointPolicy;

  private final DynamicMetricsManager _dynamicMetricsManager;
  private static final String TOTAL_EVENTS_PRODUCED = "totalEventsProduced";
  private static final String EVENTS_PRODUCED_WITHIN_SLA = "eventsProducedWithinSla";
  private static final String EVENT_PRODUCE_RATE = "eventProduceRate";
  private static final String EVENTS_LATENCY_MS_STRING = "eventsLatencyMs";

  private static final String AVAILABILITY_THRESHOLD_SLA_MS = "availabilityThresholdSlaMs";
  private static final String EVENTS_PRODUCED_OUTSIDE_SLA = "eventsProducedOutsideSla";
  private static final String AGGREGATE = "aggregate";
  private static final String DEFAULT_AVAILABILITY_THRESHOLD_SLA_MS = "60000"; // 1 minute
  private final int _availabilityThresholdSlaMs;

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

    _logger.info(String.format("Created event producer with customCheckpointing=%s", customCheckpointing));

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
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
      Validate.notNull(envelope.getKey(), "null key");
      Validate.notNull(envelope.getValue(), "null value");
    }
  }

  /**
   * Send the event onto the underlying transport.
   * @param record DatastreamEvent envelope
   * @param sendCallback
   */
  @Override
  public void send(DatastreamProducerRecord record, SendCallback sendCallback) {
    validateEventRecord(record);

    // Serialize
    record.serializeEvents(_datastreamTask.getDestinationSerDes());

    try {
      _transportProvider.send(_datastreamTask.getDatastreamDestination().getConnectionString(), record,
          (metadata, exception) -> onSendCallback(metadata, exception, sendCallback, record));
    } catch (Exception e) {
      String errorMessage = String.format("Failed send the event %s exception %s", record, e);
      _logger.warn(errorMessage, e);
      throw new DatastreamRuntimeException(errorMessage, e);
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
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, metadata.getTopic(), EVENTS_LATENCY_MS_STRING,
          sourceToDestinationLatencyMs);
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, AGGREGATE, EVENTS_LATENCY_MS_STRING,
          sourceToDestinationLatencyMs);

      if (sourceToDestinationLatencyMs <= _availabilityThresholdSlaMs) {
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, EVENTS_PRODUCED_WITHIN_SLA, 1);
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(),
            EVENTS_PRODUCED_WITHIN_SLA, 1);
      } else {
        _dynamicMetricsManager.createOrUpdateCounter(MODULE, metadata.getTopic(), EVENTS_PRODUCED_OUTSIDE_SLA, 1);
        _logger.debug(
            String.format("Event latency of %d for source %s, topic %s, partition %d exceeded SLA of %d milliseconds",
                sourceToDestinationLatencyMs, _datastreamTask.getDatastreamSource().getConnectionString(),
                metadata.getTopic(), metadata.getPartition(), _availabilityThresholdSlaMs));
      }
      _dynamicMetricsManager.createOrUpdateCounter(MODULE, AGGREGATE, TOTAL_EVENTS_PRODUCED, 1);
      _dynamicMetricsManager.createOrUpdateCounter(MODULE, _datastreamTask.getConnectorType(), TOTAL_EVENTS_PRODUCED, 1);
    }

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
    _transportProvider.flush();
    _checkpointProvider.flush();
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

    metrics.add(new BrooklinCounterInfo(className + MetricsAware.KEY_REGEX + EVENTS_PRODUCED_WITHIN_SLA));
    metrics.add(new BrooklinCounterInfo(className + MetricsAware.KEY_REGEX + TOTAL_EVENTS_PRODUCED));
    metrics.add(new BrooklinMeterInfo(className + MetricsAware.KEY_REGEX + EVENT_PRODUCE_RATE));

    metrics.add(new BrooklinHistogramInfo(className + MetricsAware.KEY_REGEX + EVENTS_LATENCY_MS_STRING));
    metrics.add(new BrooklinCounterInfo(className + MetricsAware.KEY_REGEX + EVENTS_PRODUCED_OUTSIDE_SLA));

    return Collections.unmodifiableList(metrics);
  }
}
