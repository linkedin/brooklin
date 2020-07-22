/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.connectors.kafka;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;


/**
 * KafkaTopicPartitionTracker contains information about consumer groups, topic partitions and
 * their consumer offsets.
 *
 * The information stored can then be queried via the /diag endpoint for diagnostic and analytic purposes.
 */

public class KafkaTopicPartitionTracker {

  private final String _consumerGroupId;

  private final Map<String, Set<Integer>> _topicPartitions = new ConcurrentHashMap<>();

  private final Map<String, Map<Integer, Long>> _consumerOffsets = new ConcurrentHashMap<>();

  /**
   *  Constructor for KafkaTopicPartitionTracker
   *
   * @param consumerGroupId Identifier of the consumer group
   */
  public KafkaTopicPartitionTracker(String consumerGroupId) {
    _consumerGroupId = consumerGroupId;
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

    // Remove consumer offsets for partitions that have been revoked
    topicPartitions.forEach(topicPartition -> {
      Map<Integer, Long> partitions = _consumerOffsets.get(topicPartition.topic());
      if (partitions != null) {
        partitions.remove(topicPartition.partition());
        if (partitions.isEmpty()) {
          _consumerOffsets.remove(topicPartition.topic());
        }
      }
    });
  }

  /**
   * Updates consumer offsets for partitions that have been polled
   * @param consumerRecords consumer records that have been the result of the poll
   */
  public void onPartitionsPolled(@NotNull ConsumerRecords<?, ?> consumerRecords) {
    Collection<TopicPartition> topicPartitions = consumerRecords.partitions();

    topicPartitions.forEach(topicPartition -> {
      List<? extends ConsumerRecord<?, ?>> partitionRecords = consumerRecords.records(topicPartition);
      ConsumerRecord<?, ?> lastRecord = partitionRecords.get(partitionRecords.size() - 1);

      Map<Integer, Long> partitionOffsetMap = _consumerOffsets.putIfAbsent(topicPartition.topic(),
          new ConcurrentHashMap<>());
      partitionOffsetMap.put(topicPartition.partition(), lastRecord.offset());
    });
  }

  public  Map<String, Set<Integer>> getTopicPartitions() {
    return Collections.unmodifiableMap(_topicPartitions);
  }

  /**
   * Returns a map of consumer offsets for all topic partitions
   * @return A map of consumer offsets for all topic partitions.
   */
  public Map<String, Map<Integer, Long>> getConsumerOffsets() {
    return Collections.unmodifiableMap(_consumerOffsets);
  }

  public final String getConsumerGroupId() {
    return _consumerGroupId;
  }
}