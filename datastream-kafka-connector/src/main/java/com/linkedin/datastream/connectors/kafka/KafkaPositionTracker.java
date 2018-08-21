package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.diag.PhysicalSourcePosition;
import com.linkedin.datastream.common.diag.PhysicalSources;


/**
 * KafkaPositionTracker is intended to be used with a Kafka-based connector task to keep track of the current
 * offset/position of the connector task's consumer per TopicPartition and the latest available offset/position on the
 * broker per TopicPartition.
 *
 * This information can then be used to provide diagnostic and analytic information about our position on the Kafka
 * topic (e.g. Do we have more messages to consume? Are we stuck or are we making progress?).
 */
public class KafkaPositionTracker {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPositionTracker.class);

  /**
   * Counts instantiations of this class.
   */
  private static final AtomicInteger INSTANTIATION_COUNTER = new AtomicInteger();

  /**
   * The task name of the connector this class is for.
   */
  private final String _connectorTaskName;

  /**
   * The class instantiation count of this current instantiation.
   */
  private final int _instantiationCount = INSTANTIATION_COUNTER.incrementAndGet();

  /**
   * Provides a Kafka consumer for this task.
   */
  private final Supplier<Consumer<?, ?>> _consumerSupplier;

  /**
   * The list of assigned partitions for the connector this task is for.
   */
  private final Set<TopicPartition> _assignedPartitions;

  /**
   * A check that describes if the connector task is alive. Used for closing threads associated with this task.
   */
  private final Supplier<Boolean> _isConnectorTaskAlive;

  /**
   * A store of position data for each TopicPartition. This position data is what will be returned to this task's
   * connector. The position data will be in the form of event timestamp if timestamp data is available, and otherwise
   * will use Kafka offset.
   *
   * @see com.linkedin.datastream.common.diag.PhysicalSources
   *      which will map TopicPartition -> PhysicalSourcePosition
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition
   *      which will contain position data for both the broker and this consumer
   */
  private final PhysicalSources _positions = new PhysicalSources();

  /**
   * A store of position data for each TopicPartition. This position data will exclusively be Kafka offset based, and is
   * kept to assist in calculating if the current consumer is caught-up or not.
   *
   * @see com.linkedin.datastream.common.diag.PhysicalSources
   *      which will map TopicPartition -> PhysicalSourcePosition
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition
   *      which will contain position data for both the broker and this consumer
   */
  private final PhysicalSources _offsetPositions = new PhysicalSources();

  /**
   * Executor for a service which periodically queries Kafka via RPC for the latest available Kafka offsets.
   * @see #getOffsetsFetcherRunnable()
   */
  private final ExecutorService _offsetsFetcherExecutorService;

  /**
   * Executor for the watcher for this service.
   * @see #getThreadWatcherRunnable()
   */
  private final ExecutorService _watcherService;

  /**
   * The service which periodically queries Kafka via RPC for the latest available Kafka offsets.
   * @see #getOffsetsFetcherRunnable()
   */
  private volatile Future<?> _offsetsFetcher;

  /**
   * Constructor for a KafkaPositionTracker.
   *
   * @param connectorTaskName the name of the connector task this task is for
   * @param enableBrokerOffsetFetcher If true, then we will periodically fetch query Kafka via RPC for its latest
   *                                  available offsets. If false, then this feature will not operate.
   * @param isConnectorTaskAlive A supplier that determines if the connector task this task is for is alive. If it is
   *                             not, then we should stop.
   * @param consumerSupplier a consumer supplier that is suitable for querying the brokers that the connector task is
   *                         talking to
   * @param assignedPartitions the list of assigned partitions to the connector task this task is for
   */
  public KafkaPositionTracker(final String connectorTaskName, final boolean enableBrokerOffsetFetcher,
      final Supplier<Boolean> isConnectorTaskAlive, final Supplier<Consumer<?, ?>> consumerSupplier,
      final Set<TopicPartition> assignedPartitions) {
    LOG.info("Creating KafkaPositionTracker for {} with offset fetching set to {}", connectorTaskName,
        enableBrokerOffsetFetcher);
    _connectorTaskName = connectorTaskName;
    _isConnectorTaskAlive = isConnectorTaskAlive;
    _consumerSupplier = consumerSupplier;
    _assignedPartitions = assignedPartitions;
    _offsetsFetcherExecutorService =  Executors.newFixedThreadPool(2, runnable ->
        new Thread(runnable, "offsetsFetcher-" + connectorTaskName + "-" + _instantiationCount));
    _watcherService = Executors.newSingleThreadExecutor(runnable ->
        new Thread(runnable, "offsetsFetcherWatcher-" + connectorTaskName + "-" + _instantiationCount));
    if (enableBrokerOffsetFetcher) {
      _offsetsFetcher = _offsetsFetcherExecutorService.submit(getOffsetsFetcherRunnable());
      _watcherService.submit(getThreadWatcherRunnable());
    }
  }

  /**
   * Returns a Runnable that watches/monitors, restarts, and closes Threads for this class appropriately.
   * @return a Runnable that monitors the tasks this class starts
   */
  private Runnable getThreadWatcherRunnable() {
    return () -> {
      LOG.info("Started watcher thread");
      while (_isConnectorTaskAlive.get()) {
        if (_offsetsFetcher == null || _offsetsFetcher.isDone()) {
          LOG.info("Detected that broker offsets fetcher crashed - restarting it");
          _offsetsFetcher = _offsetsFetcherExecutorService.submit(getOffsetsFetcherRunnable());
        }
        try {
          Thread.sleep(Duration.ofSeconds(1).toMillis());
        } catch (InterruptedException ignored) {
        }
      }
      LOG.info("ThreadWatcher for {} - Detected that we should be shutting down. Ending all tasks.",
          _connectorTaskName);
      _offsetsFetcher.cancel(true);
      _offsetsFetcherExecutorService.shutdown();
      _watcherService.shutdown();
    };
  }

  /**
   * Starts a service which uses an RPC to fetch the latest broker offset no more than once per minute, if that
   * information becomes out of date.
   *
   * As part of normal consumer operations, the latest broker offsets for a TopicPartition are fetched when records are
   * successfully retrieved for that topic partition. If that happens, there is no need to make a separate RPC call, as
   * we get those offsets from the Kafka consumer's metrics() API every time we get events
   * (see {@link #updateBrokerPositions(Instant, ConsumerRecords, Map, Map)}).
   *
   * However, if fetching records fails for whatever reason, then the stored information will become stale. The only way
   * for Brooklin to know for sure what the latest broker offsets are in this case is to make the RPC.
   */
  private Runnable getOffsetsFetcherRunnable() {
    return () -> {
      // We need to create a new consumer as we can't safely operate a Consumer from more than one thread.
      try (Consumer<?, ?> consumer = _consumerSupplier.get()) {
        while (_isConnectorTaskAlive.get()) {
          Future<?> future = null;
          Instant currentTime = Instant.now();

          // We want to update only those partitions which are assigned to us and haven't been updated in over 30 seconds.
          Set<TopicPartition> partitionsNeedingUpdate = _assignedPartitions.stream()
              .filter(tp -> Optional.ofNullable(_offsetPositions.get(tp.toString()))
                  .map(PhysicalSourcePosition::getSourceQueriedTimeMs)
                  .map(Instant::ofEpochMilli)
                  .orElse(Instant.EPOCH)
                  .isBefore(currentTime.minus(30, ChronoUnit.SECONDS)))
              .collect(Collectors.toSet());
          LOG.debug("Detected that the following partitions would benefit from broker endOffsets RPC: {}",
              partitionsNeedingUpdate);

          try {
            // Update the offsets, or timeout in 1 minute.
            future = _offsetsFetcherExecutorService.submit(
                updateLatestBrokerOffsetsByRpc(consumer, partitionsNeedingUpdate, currentTime.toEpochMilli()));
            future.get(1, TimeUnit.MINUTES);
            LOG.debug("Updated offsetPosition via endOffsets RPC successfully!");

            // Wait 30 seconds before running this operation again.
            Thread.sleep(Duration.ofSeconds(30).toMillis());
          } catch (Exception e) {
            if (future != null) {
              future.cancel(true);
            }
            LOG.warn("Failed to update broker end offsets via RPC.", e);
          }
        }
        LOG.info("OffsetsFetcher for {} - Detected that we should be shutting down. Closing the consumer.",
            _connectorTaskName);
      }
    };
  }

  /**
   * Uses the specified consumer to make an RPC call to get the end offsets for the specified partitions and updates the
   * metadata. Use externally for testing purposes only. Note that the provided consumer must not be either the consumer
   * for this task or the consumer used within the endOffsetsUpdater task, or else a concurrent modification condition
   * may arise.
   *
   * @param consumer the specified consumer
   * @param partitions the partitions to fetch broker offsets for
   * @param currentTime the time the operation is asked for
   * @return a Runnable which will perform the specified operation
   */
  @VisibleForTesting
  Runnable updateLatestBrokerOffsetsByRpc(Consumer<?, ?> consumer, Set<TopicPartition> partitions, long currentTime) {
    LOG.debug("Updating the offsetPosition via endOffsets RPC for partitions {} for time {} for task {}", partitions,
        currentTime, _connectorTaskName);
    return () -> consumer.endOffsets(partitions).forEach((tp, offset) -> {
      PhysicalSourcePosition offsetPosition = new PhysicalSourcePosition();
      offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
      offsetPosition.setSourceQueriedTimeMs(currentTime);
      offsetPosition.setSourcePosition(String.valueOf(offset));
      LOG.debug("Adding endOffsets calculated offsetPosition {} for partition {}", offsetPosition, tp);
      _offsetPositions.update(tp.toString(), offsetPosition);
      LOG.debug("New offsetPosition for partition {} is {}", tp, _offsetPositions.get(tp.toString()));

      PhysicalSourcePosition position = _positions.get(tp.toString());
      if (position != null && position.getPositionType().equals(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE)) {
        LOG.debug("Updating the source position for partition {} via endOffsets RPC due to "
            + "kafkaOffset position type being used", tp);
        position.setSourceQueriedTimeMs(currentTime);
        position.setSourcePosition(String.valueOf(offset));
      }
    });
  }

  /**
   * Updates the initial positions with offset data from the consumer and broker.
   * @param consumerOffsets a map of TopicPartitions to offsets for the consumer
   * @param brokerOffsets a map of TopicPartitions to offsets for the broker
   */
  public void updateInitialPositions(Instant readTime, Map<TopicPartition, Long> consumerOffsets,
      Map<TopicPartition, Long> brokerOffsets) {
    LOG.debug("Update initial positions called at {} for partitions {}", consumerOffsets.keySet());
    consumerOffsets.forEach((tp, offset) -> {
      Supplier<PhysicalSourcePosition> initialPosition = () -> {
        PhysicalSourcePosition position = new PhysicalSourcePosition();
        position.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
        position.setSourceQueriedTimeMs(readTime.toEpochMilli());
        position.setSourcePosition(String.valueOf(brokerOffsets.get(tp)));
        position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
        position.setConsumerPosition(String.valueOf(offset));
        return position;
      };
      if (_offsetPositions.get(tp.toString()) == null) {
        _offsetPositions.update(tp.toString(), initialPosition.get());
      }
      if (_positions.get(tp.toString()) == null) {
        _positions.update(tp.toString(), initialPosition.get());
      }
    });
  }

  /**
   * Updates the latest consumer and broker offsets based on the position reported by the consumer for the records
   * received, and the internal Kafka consumer metrics.
   *
   * @param records the records fetched in the current poll
   * @param readTime the time the records were fetched
   * @param metrics the metrics for the Kafka consumer
   * @param offsets the current position for the Kafka consumer
   */
  public void updatePositions(Instant readTime, ConsumerRecords<?, ?> records,
      Map<MetricName, ? extends Metric> metrics, Map<TopicPartition, Long> offsets) {
    LOG.debug("Update partitions called at {} for records received with partitions {}", readTime, records.partitions());
    updateBrokerPositions(readTime, records, metrics, offsets);
    for (ConsumerRecord<?, ?> record : records) {
      updateConsumerPositions(readTime, record);
    }
  }

  /**
   * Updates the latest broker offsets based on internal Kafka consumer metrics. Since the consumer metrics are only
   * updated per topic partition actually fetched, we update only those positions.
   *
   * @param records the records fetched in the current poll
   * @param readTime the time the records were fetched
   * @param metrics the metrics for the Kafka consumer
   * @param offsets the current position for the Kafka consumer
   */
  private void updateBrokerPositions(Instant readTime, ConsumerRecords<?, ?> records,
      Map<MetricName, ? extends Metric> metrics, Map<TopicPartition, Long> offsets) {
    LOG.debug("Attempting to update broker positions at {} for records received with partitions {}", readTime,
        records.partitions());

    // Metric names per KIP-92, which applies to Kafka versions >= 0.10.2.0 and < 1.1.0
    BiPredicate<MetricName, TopicPartition> matchesKip92 =
        (metricName, topicPartition) -> metricName != null && metricName.name() != null && metricName.name()
            .equals(topicPartition + ".records-lag");
    // Metric names per KIP-225, which applies to Kafka versions >= 1.1.0
    BiPredicate<MetricName, TopicPartition> matchesKip225 =
        (metricName, topicPartition) -> metricName != null && metricName.name() != null && metricName.name()
            .equals("records-lag") && metricName.tags() != null && metricName.tags().containsKey("topic")
            && metricName.tags().get("topic").equals(topicPartition.topic()) && metricName.tags()
            .containsKey("partition") && metricName.tags()
            .get("partition")
            .equals(String.valueOf(topicPartition.partition()));

    for (TopicPartition topicPartition : records.partitions()) {
      for (MetricName metricName : metrics.keySet()) {
        if (matchesKip92.or(matchesKip225).test(metricName, topicPartition)) {
          LOG.debug("Metric {} measures record lag for partition {}", metricName, topicPartition);

          // Calculate what the broker position must be from the lag metric
          long consumerLag = (long) metrics.get(metricName).value();
          long consumerPosition = offsets.get(topicPartition);
          long brokerPosition = consumerPosition + consumerLag;
          LOG.debug("Partition {} has consumer lag {}, consumer position {}, and brokerPosition {} at time {}",
              topicPartition, consumerLag, consumerPosition, brokerPosition, readTime);

          // Build and update the broker offset position data
          PhysicalSourcePosition offsetPosition = new PhysicalSourcePosition();
          offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
          offsetPosition.setSourceQueriedTimeMs(readTime.toEpochMilli());
          offsetPosition.setSourcePosition(Long.toString(brokerPosition));
          LOG.debug("Adding metrics calculated offsetPosition {} for partition {}", offsetPosition, topicPartition);
          _offsetPositions.update(topicPartition.toString(), offsetPosition);
          LOG.debug("New offsetPosition for partition {} is {}", topicPartition,
              _offsetPositions.get(topicPartition.toString()));
        }
      }
    }
  }

  /**
   * Updates the current consumer position from the record we are currently reading.
   *
   * @param record the record we are currently reading
   * @param readTime the time the records were fetched
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition for information on what a position is
   */
  private void updateConsumerPositions(Instant readTime, ConsumerRecord<?, ?> record) {
    String physicalSource = new TopicPartition(record.topic(), record.partition()).toString();

    // Why do we add +1 to the record's offset? All the other Kafka APIs involved here from position() to endOffsets()
    // add +1. So, it is just a little more convenient to modify the position metadata we are storing here than it is
    // to handle the way all the other Kafka APIs return values.
    String offset = Long.toString(record.offset() + 1);

    // Update offset position. This is used to check for caught-up partitions when the response is actually returned.
    // See getPositionResponse() for details.
    PhysicalSourcePosition offsetPosition = new PhysicalSourcePosition();
    offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
    offsetPosition.setConsumerProcessedTimeMs(readTime.toEpochMilli());
    offsetPosition.setConsumerPosition(offset);
    LOG.debug("Adding record received calculated offsetPosition {} for partition {}", offsetPosition, physicalSource);
    _offsetPositions.update(physicalSource, offsetPosition);
    LOG.debug("New offsetPosition for partition {} is {}", physicalSource, _offsetPositions.get(physicalSource));

    PhysicalSourcePosition position = new PhysicalSourcePosition();
    if (record.timestampType() != null && record.timestampType() == TimestampType.LOG_APPEND_TIME
        && record.timestamp() >= 0) {
      // If the event timestamp is available, let's use that.
      position.setPositionType(PhysicalSourcePosition.EVENT_TIME_POSTIION_TYPE);
      position.setSourceQueriedTimeMs(readTime.toEpochMilli());
      position.setSourcePosition(Long.toString(readTime.toEpochMilli()));
      position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
      position.setConsumerPosition(Long.toString(record.timestamp()));
    } else {
      // If the event timestamp isn't available, we'll use Kafka offset data instead.
      position.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
      position.setSourceQueriedTimeMs(_offsetPositions.get(physicalSource).getSourceQueriedTimeMs());
      position.setSourcePosition(_offsetPositions.get(physicalSource).getSourcePosition());
      position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
      position.setConsumerPosition(offset);
    }
    LOG.debug("Adding record received calculated position {} for partition {}", position, physicalSource);
    _positions.update(physicalSource, position);
    LOG.debug("New position for partition {} is {}", position, _positions.get(physicalSource));
  }

  /**
   * Gets a DatastreamPositionResponse containing position data for the current task.
   * @return the current position data
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition for information on what a position is
   */
  public PhysicalSources getPositions() {
    // Update the position data for any caught-up partitions
    _offsetPositions.getPhysicalSourceToPosition().forEach((tp, offsetPosition) -> {
      // Skip updating the position data if neither can be resolved
      if (offsetPosition.getConsumerPosition() == null || offsetPosition.getSourcePosition() == null) {
        return;
      }

      long consumerPosition = Long.parseLong(offsetPosition.getConsumerPosition()); // Our consumer's offset
      long sourcePosition = Long.parseLong(offsetPosition.getSourcePosition()); // The broker's last offset
      long consumerProcessedTimeMs = offsetPosition.getConsumerProcessedTimeMs(); // The last time we processed an event
      long sourceQueriedTimeMs = offsetPosition.getSourceQueriedTimeMs(); // The last time we fetched the broker's last offset data

      if (sourceQueriedTimeMs > consumerProcessedTimeMs) {
        if (sourcePosition == consumerPosition) {
          LOG.debug("Detected that we are caught up for partition {} so updating position.", tp);

          // We are caught-up -- our consumer position matches the broker position.
          PhysicalSourcePosition position = _positions.get(tp);

          // We want to update our current position to describe this caught up state since our consumer position data is
          // more stale than the broker position data we have. How do we do this?
          //
          // We imagine there is an imaginary 'heartbeat' event which doesn't have an offset (since it's imaginary)
          // that's positioned at the last time we successfully fetched broker offsets, and we update the metadata
          // accordingly.

          // If we are using event time positions, then we should update our event time position.
          if (position.getPositionType().equals(PhysicalSourcePosition.EVENT_TIME_POSTIION_TYPE)) {
            LOG.debug("Updating consumerPosition for partition {} to sourceQueriedTimeMs {}", tp, sourceQueriedTimeMs);
            position.setConsumerPosition(Long.toString(sourceQueriedTimeMs));
          }
          // If we aren't using event times (we are using offsets), then we shouldn't modify the value.

          // We update our last processed time to the last time we fetched the broker's position data.
          LOG.debug("Updating consumerProcessedTimeMs for partition {} to sourceQueriedTimeMs {}", tp,
              sourceQueriedTimeMs);
          position.setConsumerProcessedTimeMs(sourceQueriedTimeMs);
          LOG.debug("New position for partition {} is {}", _positions.get(tp));
        }
      }
    });

    return _positions;
  }

  /**
   * Removes data for all but the specified topic partitions.
   * @param topicPartitions the specified topic partitions
   */
  public void retainAll(Set<TopicPartition> topicPartitions) {
    LOG.debug("Removing all topic partitions besides {} from positions and offsetPositions", topicPartitions);
    _positions.retainAll(topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.toList()));
    _offsetPositions.retainAll(topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.toList()));
  }
}