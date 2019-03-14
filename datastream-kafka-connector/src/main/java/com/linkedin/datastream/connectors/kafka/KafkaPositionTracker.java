/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DurableScheduledService;
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
public class  KafkaPositionTracker {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPositionTracker.class);

  /**
   * The task name of the connector this class is for.
   */
  private final String _connectorTaskName;

  /**
   * The list of assigned partitions for the connector this task is for.
   */
  private final Set<TopicPartition> _assignedPartitions;

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
   * The frequency at which to fetch offsets from the broker using an RPC call.
   */
  private static final Duration OFFSETS_FETCH_INTERVAL = Duration.ofSeconds(30);

  /**
   * The maximum duration from the last terminated endOffsets RPCs calls to when the Kafka consumer is assumed to be
   * faulty and is reconstructed.
   */
  private static final Duration LAST_CONSUMER_RPC_TERMINATED_TIMEOUT = Duration.ofMinutes(5);

  /**
   * The service responsible for fetching offsets.
   */
  private DurableScheduledService _offsetsFetcherService; // Defined to help investigation issues (when you have a heap
  // dump or are in a debugger)

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
    _assignedPartitions = assignedPartitions;
    if (enableBrokerOffsetFetcher) {
      _offsetsFetcherService = new DurableScheduledService(_connectorTaskName, OFFSETS_FETCH_INTERVAL,
          LAST_CONSUMER_RPC_TERMINATED_TIMEOUT) {
        private Consumer<?, ?> _consumer;

        @Override
        protected void startUp() {
          _consumer = consumerSupplier.get();
        }

        @Override
        protected void runOneIteration() {
          final Instant currentTime = Instant.now();

          // Make a quick copy of our assigned partitions (we share it with the KafkaConnectorTask thread, so it isn't
          // thread-safe).
          final Set<TopicPartition> assignedPartitions = ImmutableSet.copyOf(_assignedPartitions);

          // We want to update only those partitions which are assigned to us and haven't been updated in our fetch
          // interval.
          final Set<TopicPartition> partitionsNeedingUpdate = assignedPartitions.stream()
              .filter(tp -> Optional.ofNullable(_offsetPositions.get(tp.toString()))
                  .map(PhysicalSourcePosition::getSourceQueriedTimeMs)
                  .map(Instant::ofEpochMilli)
                  .orElse(Instant.EPOCH)
                  .isBefore(currentTime.minus(OFFSETS_FETCH_INTERVAL)))
              .collect(Collectors.toSet());

          // If we are caught up on all partitions, there is no need to make any RPC calls.
          if (partitionsNeedingUpdate.isEmpty()) {
            return;
          }

          LOG.debug("Detected that the following partitions would benefit from broker endOffsets RPC: {}",
              partitionsNeedingUpdate);

          try {
            updateLatestBrokerOffsetsByRpc(_consumer, partitionsNeedingUpdate, currentTime.toEpochMilli());
          } catch (Exception e) {
            // If we run into an exception, we should crash and allow ourselves to be revived by the
            // DurableScheduledService
            LOG.warn("Failed to update broker end offsets via RPC.", e);
            throw e;
          }
        }

        @Override
        protected void shutDown() {
          if (_consumer != null) {
            _consumer.close();
          }
        }

        @Override
        protected boolean hasLeaked() {
          return !isConnectorTaskAlive.get();
        }
      };
      _offsetsFetcherService.startAsync();
    }
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
   */
  @VisibleForTesting
  void updateLatestBrokerOffsetsByRpc(final Consumer<?, ?> consumer, final Set<TopicPartition> partitions,
      final long currentTime) {
    LOG.debug("Updating the offsetPosition via endOffsets RPC for partitions {} for time {} for task {}", partitions,
        currentTime, _connectorTaskName);

    // Update the offset positions with the result of the RPC call
    consumer.endOffsets(partitions).forEach((tp, offset) -> {
      PhysicalSourcePosition offsetPosition = _offsetPositions.get(tp.toString());
      if (offsetPosition == null) {
        offsetPosition = new PhysicalSourcePosition();
        _offsetPositions.set(tp.toString(), offsetPosition);
      }
      offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
      offsetPosition.setSourceQueriedTimeMs(currentTime);
      offsetPosition.setSourcePosition(String.valueOf(offset));
      LOG.debug("New offsetPosition for partition {} is {}", tp, offsetPosition);

      final PhysicalSourcePosition position = _positions.get(tp.toString());
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
   * @param consumer the consumer to use for fetching the consumer and broker positions for all assignments
   * @return true if the initialization was successful, false if it should be retried
   */
  public boolean initializePositions(final Consumer<?, ?> consumer) {
    // Get list of partitions needing position init
    final List<TopicPartition> partitionsNeedingInit = consumer.assignment()
        .stream()
        .filter(tp -> getPositions().get(tp.toString()) == null)
        .collect(Collectors.toList());

    // Initialize any partitions needed
    if (!partitionsNeedingInit.isEmpty()) {
      boolean noError = true;
      final Instant readTime = Instant.now();

      LOG.debug("Attempting to initialize positions for {}", partitionsNeedingInit);

      // Get the consumer offsets
      final Map<TopicPartition, Long> consumerOffsets = new HashMap<>();
      for (TopicPartition tp : partitionsNeedingInit) {
        try {
          consumerOffsets.put(tp, consumer.position(tp));
        } catch (Exception e) {
          LOG.warn("Failed to get consumer offsets for {}", tp, e);
          noError = false; // If we failed to get any of them, we have an error
        }
      }

      if (consumerOffsets.isEmpty()) {
        return false;
      }

      // Get the broker offsets
      final Map<TopicPartition, Long> brokerOffsets;
      try {
        brokerOffsets = consumer.endOffsets(consumerOffsets.keySet());
      } catch (Exception e) {
        LOG.warn("Failed to get broker offsets for partitions {}", consumerOffsets.keySet(), e);
        return false;
      }

      // Initialize
      consumerOffsets.forEach((tp, offset) -> {
        final PhysicalSourcePosition offsetPosition = new PhysicalSourcePosition();
        offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
        offsetPosition.setSourceQueriedTimeMs(readTime.toEpochMilli());
        offsetPosition.setSourcePosition(String.valueOf(brokerOffsets.get(tp)));
        offsetPosition.setConsumerProcessedTimeMs(readTime.toEpochMilli());
        offsetPosition.setConsumerPosition(String.valueOf(offset));
        _offsetPositions.set(tp.toString(), offsetPosition);

        final PhysicalSourcePosition position = new PhysicalSourcePosition();
        position.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
        position.setSourceQueriedTimeMs(readTime.toEpochMilli());
        position.setSourcePosition(String.valueOf(brokerOffsets.get(tp)));
        position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
        position.setConsumerPosition(String.valueOf(offset));
        _positions.set(tp.toString(), position);
      });

      return noError;
    }

    return true;
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
  public void updatePositions(final Instant readTime, final ConsumerRecords<?, ?> records,
      final Map<MetricName, ? extends Metric> metrics, final Map<TopicPartition, Long> offsets) {
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
  private void updateBrokerPositions(final Instant readTime, final ConsumerRecords<?, ?> records,
      Map<MetricName, ? extends Metric> metrics, Map<TopicPartition, Long> offsets) {
    LOG.debug("Attempting to update broker positions at {} for records received with partitions {}", readTime,
        records.partitions());

    // Metric names per KIP-92, which applies to Kafka versions >= 0.10.2.0 and < 1.1.0
    final BiPredicate<MetricName, TopicPartition> matchesKip92 =
        (metricName, topicPartition) -> metricName != null && metricName.name() != null && metricName.name()
            .equals(topicPartition + ".records-lag");
    // Metric names per KIP-225, which applies to Kafka versions >= 1.1.0
    final BiPredicate<MetricName, TopicPartition> matchesKip225 =
        (metricName, topicPartition) -> metricName != null && metricName.name() != null && metricName.name()
            .equals("records-lag") && metricName.tags() != null && metricName.tags().containsKey("topic")
            && metricName.tags().get("topic").equals(topicPartition.topic()) && metricName.tags()
            .containsKey("partition") && metricName.tags()
            .get("partition")
            .equals(String.valueOf(topicPartition.partition()));

    for (final TopicPartition topicPartition : records.partitions()) {
      for (final MetricName metricName : metrics.keySet()) {
        if (matchesKip92.or(matchesKip225).test(metricName, topicPartition)) {
          LOG.debug("Metric {} measures record lag for partition {}", metricName, topicPartition);

          // Calculate what the broker position must be from the lag metric
          final long consumerLag = (long) metrics.get(metricName).value();
          final long consumerPosition = offsets.get(topicPartition);
          final long brokerPosition = consumerPosition + consumerLag;
          LOG.debug("Partition {} has consumer lag {}, consumer position {}, and brokerPosition {} at time {}",
              topicPartition, consumerLag, consumerPosition, brokerPosition, readTime);

          // Build and update the broker offset position data
          PhysicalSourcePosition offsetPosition = _offsetPositions.get(topicPartition.toString());
          if (offsetPosition == null) {
            offsetPosition = new PhysicalSourcePosition();
            _offsetPositions.set(topicPartition.toString(), offsetPosition);
          }
          offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
          offsetPosition.setSourceQueriedTimeMs(readTime.toEpochMilli());
          offsetPosition.setSourcePosition(Long.toString(brokerPosition));
          LOG.debug("New offsetPosition for partition {} is {}", topicPartition, offsetPosition);
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
  private void updateConsumerPositions(final Instant readTime, final ConsumerRecord<?, ?> record) {
    final String physicalSource = new TopicPartition(record.topic(), record.partition()).toString();

    // Why do we add +1 to the record's offset? All the other Kafka APIs involved here from position() to endOffsets()
    // add +1. So, it is just a little more convenient to modify the position metadata we are storing here than it is
    // to handle the way all the other Kafka APIs return values.
    final String offset = Long.toString(record.offset() + 1);

    // Update offset position. This is used to check for caught-up partitions when the response is actually returned.
    // See getPositionResponse() for details.
    PhysicalSourcePosition offsetPosition = _offsetPositions.get(physicalSource);
    if (offsetPosition == null) {
      offsetPosition = new PhysicalSourcePosition();
      _offsetPositions.set(physicalSource, offsetPosition);
    }
    offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
    offsetPosition.setConsumerProcessedTimeMs(readTime.toEpochMilli());
    offsetPosition.setConsumerPosition(offset);
    LOG.debug("New offsetPosition for partition {} is {}", physicalSource, physicalSource);

    PhysicalSourcePosition position = _positions.get(physicalSource);
    if (position == null) {
      position = new PhysicalSourcePosition();
      _positions.set(physicalSource, position);
    }
    if (record.timestampType() != null && record.timestampType() == TimestampType.LOG_APPEND_TIME
        && record.timestamp() >= 0) {
      // If the event timestamp is available, let's use that.
      position.setPositionType(PhysicalSourcePosition.EVENT_TIME_POSITION_TYPE);
      position.setSourceQueriedTimeMs(readTime.toEpochMilli());
      position.setSourcePosition(Long.toString(readTime.toEpochMilli()));
      position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
      position.setConsumerPosition(Long.toString(record.timestamp()));
    } else {
      // If the event timestamp isn't available, we'll use Kafka offset data instead.
      position.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
      position.setSourceQueriedTimeMs(offsetPosition.getSourceQueriedTimeMs());
      position.setSourcePosition(offsetPosition.getSourcePosition());
      position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
      position.setConsumerPosition(offset);
    }
    LOG.debug("New position for partition {} is {}", physicalSource, position);
  }

  /**
   * Gets a PhysicalSources object containing offset-based position data for the current task.
   * @return the current offset-based position data
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition for information on what a position is
   */
  public PhysicalSources getOffsetPositions() {
    return _offsetPositions;
  }

  /**
   * Gets a PhysicalSources object containing time-based position data for the current task.
   * @return the current time-based position data
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition for information on what a position is
   */
  public PhysicalSources getPositions() {
    // Update the position data for any caught-up partitions
    _offsetPositions.getPhysicalSourceToPosition().forEach((tp, offsetPosition) -> {
      // Skip updating the position data if neither can be resolved
      if (offsetPosition.getConsumerPosition() == null || offsetPosition.getSourcePosition() == null) {
        return;
      }

      final long consumerPosition = Long.parseLong(offsetPosition.getConsumerPosition()); // Our consumer's offset
      final long sourcePosition = Long.parseLong(offsetPosition.getSourcePosition()); // The broker's last offset
      final long consumerProcessedTimeMs = offsetPosition.getConsumerProcessedTimeMs(); // The last time we processed an event
      final long sourceQueriedTimeMs = offsetPosition.getSourceQueriedTimeMs(); // The last time we fetched the broker's last offset data

      if (sourceQueriedTimeMs > consumerProcessedTimeMs) {
        if (sourcePosition == consumerPosition) {
          LOG.debug("Detected that we are caught up for partition {} so updating position.", tp);

          // We are caught-up -- our consumer position matches the broker position.

          PhysicalSourcePosition position = _positions.get(tp);
          if (position == null) {
            // If this has occurred, the best we can do is match the data in our offset position
            position = new PhysicalSourcePosition();
            position.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
            position.setConsumerProcessedTimeMs(consumerProcessedTimeMs);
            position.setConsumerPosition(String.valueOf(consumerPosition));
            position.setSourceQueriedTimeMs(sourceQueriedTimeMs);
            position.setSourcePosition(String.valueOf(sourcePosition));
            _positions.set(tp, position);

            LOG.debug("Position was missing for partition {} so we filled it in with the offset position data.", tp);
            return;
          }

          LOG.debug("Current position for partition {} is {}", tp, position);

          // Fill in/update the source position data
          position.setSourceQueriedTimeMs(sourceQueriedTimeMs);
          if (position.getPositionType().equals(PhysicalSourcePosition.EVENT_TIME_POSITION_TYPE)) {
            position.setSourcePosition(String.valueOf(sourceQueriedTimeMs));
          } else {
            position.setSourcePosition(String.valueOf(sourcePosition));
          }

          // We want to update our current position to describe this caught up state since our consumer position data is
          // more stale than the broker position data we have. How do we do this?
          //
          // We imagine there is an imaginary 'heartbeat' event which doesn't have an offset (since it's imaginary)
          // that's positioned at the last time we successfully fetched broker offsets, and we update the metadata
          // accordingly.

          if (position.getPositionType().equals(PhysicalSourcePosition.EVENT_TIME_POSITION_TYPE)) {
            // If we are using event time positions, then we should update our event time position.
            position.setConsumerPosition(Long.toString(sourceQueriedTimeMs));
          }
          // If we aren't using event times (we are using offsets), then we shouldn't modify the value.

          // We update our last processed time to the last time we fetched the broker's position data.
          position.setConsumerProcessedTimeMs(sourceQueriedTimeMs);

          LOG.debug("After setting, position for partition {} is {}", tp, position);
        }
      }
    });

    return _positions;
  }

  /**
   * Removes data for all but the specified topic partitions.
   * @param topicPartitions the specified topic partitions
   */
  public void retainAll(final Set<TopicPartition> topicPartitions) {
    LOG.debug("Removing all topic partitions besides {} from positions and offsetPositions", topicPartitions);
    _positions.retainAll(topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.toList()));
    _offsetPositions.retainAll(topicPartitions.stream().map(TopicPartition::toString).collect(Collectors.toList()));
  }
}