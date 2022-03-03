/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Response structure used for Topic partition stats
 */
public class KafkaTopicPartitionStatsResponse {

  private final String _sourceInstance;

  private final String _consumerGroupId;

  private final Map<String, Set<Integer>> _topicPartitions;

  private final Set<String> _datastreams;

  /**
   * Constructor for KafkaTopicPartitionStatsResponse
   */
  public KafkaTopicPartitionStatsResponse(String consumerGroupId) {
    _consumerGroupId = consumerGroupId;
    _topicPartitions = new HashMap<>();
    _datastreams = new HashSet<>();
    _sourceInstance = null;
  }

  /**
   * Constructor for KafkaTopicPartitionStatsResponse
   * @param consumerGroupId identifier for consumer group
   * @param topicPartitions a map of consumer offsets for topic partitions
   */
  @JsonCreator
  public KafkaTopicPartitionStatsResponse(@JsonProperty("consumerGroupId") String consumerGroupId,
      @JsonProperty("sourceInstance") String sourceInstance,
      @JsonProperty("topicPartitions") Map<String, Set<Integer>> topicPartitions,
      @JsonProperty("datastreams") Set<String> datastreams) {
    _consumerGroupId = consumerGroupId;
    _sourceInstance = sourceInstance;
    _topicPartitions = topicPartitions;
    _datastreams = datastreams;
  }

  public String getConsumerGroupId() {
    return _consumerGroupId;
  }

  public String getSourceInstance() {
    return _sourceInstance;
  }

  public Map<String, Set<Integer>> getTopicPartitions() {
    return _topicPartitions;
  }

  public Set<String> getDatastreams() {
    return _datastreams;
  }
}
