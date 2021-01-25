/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.connectors.kafka;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.jetbrains.annotations.NotNull;


/**
 * KafkaTopicPartitionTracker contains information about consumer groups, topic partitions and
 * their consumer offsets.
 *
 * The information stored can then be queried via the /diag endpoint for diagnostic and analytic purposes.
 */

public class KafkaTopicPartitionTracker {

  private final String _consumerGroupId;
  private final String _datastreamName;

  private final Map<String, Set<Integer>> _topicPartitions = new ConcurrentHashMap<>();
  private final Map<String, Map<Integer, Long>> _consumedOffsets = new ConcurrentHashMap<>();
  private final Map<String, Map<Integer, Long>> _committedOffsets = new ConcurrentHashMap<>();
  private final Map<String, Map<Integer, Long>> _consumptionLag = new ConcurrentHashMap<>();

  /**
   *  Constructor for KafkaTopicPartitionTracker
   *
   * @param consumerGroupId Identifier of the consumer group
   * @param datastreamName Name of the datastream
   */
  public KafkaTopicPartitionTracker(String consumerGroupId, String datastreamName) {
    _consumerGroupId = consumerGroupId;
    _datastreamName = datastreamName;
  }

  /**
   * Assigns paritions. This method should be called whenever the Connector's consumer
   * finishes assigning partitions.
   *
   * @param topicPartitions the topic partitions which have been assigned
   */
  public void onPartitionsAssigned(@NotNull Collection<TopicPartition> topicPartitions) {
    // updating topic partitions
    topicPartitions.forEach(partition -> {
      Set<Integer> partitions = _topicPartitions.computeIfAbsent(partition.topic(), k -> new HashSet<>());
      partitions.add(partition.partition());
    });
  }

  /**
   * Frees partitions that have been revoked. This method should be called whenever the Connector's
   * consumer is about to re-balance (and thus unassign partitions).
   *
   * @param topicPartitions the topic partitions which were previously assigned
   */
  public void onPartitionsRevoked(@NotNull Collection<TopicPartition> topicPartitions) {
    topicPartitions.forEach(partition -> {
      Set<Integer> partitions = _topicPartitions.get(partition.topic());
      if (partitions != null) {
        partitions.remove(partition.partition());
        if (partitions.isEmpty()) {
          _topicPartitions.remove(partition.topic());
        }
      }
    });

    // Remove consumed offsets for partitions that have been revoked. The reason to remove the consumed offsets
    // here is that another host may handle these partitions due to rebalance, and we don't want to have duplicate
    // consumer offsets for affected partitions (even though the ones with larger offsets wins).
    cleanupTopicPartitionsFromMap(topicPartitions, _consumedOffsets);

    // Remove committed offsets for partitions that have been revoked.
    cleanupTopicPartitionsFromMap(topicPartitions, _committedOffsets);

    // Remove consumption lag data for partitions that have been revoked.
    cleanupTopicPartitionsFromMap(topicPartitions, _consumptionLag);
  }

  private void cleanupTopicPartitionsFromMap(@NotNull Collection<TopicPartition> topicPartitions,
      Map<String, Map<Integer, Long>> map) {
    topicPartitions.forEach(topicPartition -> {
      Map<Integer, Long> partitions = map.get(topicPartition.topic());
      if (partitions != null) {
        partitions.remove(topicPartition.partition());
        if (partitions.isEmpty()) {
          map.remove(topicPartition.topic());
        }
      }
    });
  }

  /**
   * Updates consumed offsets for partitions that have been polled
   * @param consumerRecords consumer records that have been the result of the poll
   */
  public void onPartitionsPolled(@NotNull ConsumerRecords<?, ?> consumerRecords) {
    Collection<TopicPartition> topicPartitions = consumerRecords.partitions();

    topicPartitions.forEach(topicPartition -> {
      List<? extends ConsumerRecord<?, ?>> partitionRecords = consumerRecords.records(topicPartition);
      ConsumerRecord<?, ?> lastRecord = partitionRecords.get(partitionRecords.size() - 1);

      Map<Integer, Long> partitionOffsetMap = _consumedOffsets.computeIfAbsent(topicPartition.topic(),
          k -> new ConcurrentHashMap<>());
      partitionOffsetMap.put(topicPartition.partition(), lastRecord.offset());

      Map<Integer, Long> partitionConsumptionLagMap = _consumptionLag.computeIfAbsent(topicPartition.topic(),
          k -> new ConcurrentHashMap<>());
      partitionConsumptionLagMap.put(topicPartition.partition(), calculateLag(lastRecord));
    });
  }

  private Long calculateLag(ConsumerRecord<?, ?> record) {
    if (record.timestampType().equals(TimestampType.LOG_APPEND_TIME)) {
      return Instant.now().toEpochMilli() - record.timestamp();
    }
    // No meaningful lag can be calculated if the timestamp is not log append time
    return -1L;
  }

  /**
   * Updates committed offsets for topic partitions
   * @param offsetMap offsets for topic partitions that have been committed
   */
  public void onOffsetsCommitted(Map<TopicPartition, OffsetAndMetadata> offsetMap) {
    Collection<TopicPartition> topicPartitions = offsetMap.keySet();

    topicPartitions.forEach(topicPartition -> {
      Map<Integer, Long> partitionOffsetMap = _committedOffsets.computeIfAbsent(topicPartition.topic(),
          k -> new ConcurrentHashMap<>());
      partitionOffsetMap.put(topicPartition.partition(), offsetMap.get(topicPartition).offset());
    });
  }

  public  Map<String, Set<Integer>> getTopicPartitions() {
    return Collections.unmodifiableMap(_topicPartitions);
  }

  /**
   * Returns a map of consumed offsets for all topic partitions
   */
  public Map<String, Map<Integer, Long>> getConsumedOffsets() {
    return Collections.unmodifiableMap(_consumedOffsets);
  }

  /**
   * Returns a map of committed offsets for all topic partitions
   */
  public Map<String, Map<Integer, Long>> getCommittedOffsets() {
    return Collections.unmodifiableMap(_committedOffsets);
  }

  /**
   * Returns a map of consumption lag (in milliseconds) for all topic partitions
   */
  public Map<String, Map<Integer, Long>> getConsumptionLag() {
    return Collections.unmodifiableMap(_consumptionLag);
  }

  /**
   * Gets the identifier for consumer group
   */
  public final String getConsumerGroupId() {
    return _consumerGroupId;
  }

  /**
   * Gets the datastream name
   */
  public final String getDatastreamName() {
    return _datastreamName;
  }
}