/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.connectors.kafka;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;

import com.linkedin.datastream.common.JsonUtils;


/**
 * Utility class for Kafka based connectors
 */
public class KafkaConnectorDiagUtils {
  /**
   * Reduce/Merge the KafkaTopicPartitionStatsResponse responses of a collection of host/instance into one response
   */
  public static String reduceTopicPartitionStatsResponses(Map<String, String> responses, Logger logger) {
    Map<String, KafkaTopicPartitionStatsResponse> result = new HashMap<>();

    responses.forEach((instance, json) -> {
      List<KafkaTopicPartitionStatsResponse> responseList;
      try {
        responseList = JsonUtils.fromJson(json, new TypeReference<List<KafkaTopicPartitionStatsResponse>>() {
        });
      } catch (Exception e) {
        logger.error("Invalid response {} from instance {}", json, instance);
        return;
      }

      responseList.forEach(response -> {
        if (response.getTopicPartitions() == null || StringUtils.isBlank(response.getConsumerGroupId())
            || response.getDatastreams() == null) {
          logger.warn("Empty topic partition stats map from instance {}. Ignoring the result", instance);
          return;
        }

        KafkaTopicPartitionStatsResponse reducedResponse = result.computeIfAbsent(response.getConsumerGroupId(),
            k -> new KafkaTopicPartitionStatsResponse(response.getConsumerGroupId()));
        reducedResponse.getDatastreams().addAll(response.getDatastreams());

        Map<String, Set<Integer>> topicPartitions = response.getTopicPartitions();
        topicPartitions.forEach((topic, partitions) -> {
          Map<String, Set<Integer>> reducedTopicPartitions = reducedResponse.getTopicPartitions();
          Set<Integer> reducedPartitions = reducedTopicPartitions.computeIfAbsent(topic, k -> new HashSet<>());
          reducedPartitions.addAll(partitions);
        });
      });
    });

    return JsonUtils.toJson(result.values());
  }

  /**
   * Reduce/Merge the KafkaConsumerOffsetsResponse responses of a collection of hosts/instances into one response
   */
  public static String reduceConsumerOffsetsResponses(Map<String, String> responses, Logger logger) {
    Map<String, KafkaConsumerOffsetsResponse> result = new HashMap<>();

    responses.forEach((instance, json) -> {
      List<KafkaConsumerOffsetsResponse> responseList;
      try {
        responseList = JsonUtils.fromJson(json, new TypeReference<List<KafkaConsumerOffsetsResponse>>() {
        });
      } catch (Exception e) {
        logger.error("Invalid response {} from instance {}", json, instance);
        return;
      }

      responseList.forEach(response -> {
        if (response.getConsumerOffsets() == null || StringUtils.isBlank(response.getConsumerGroupId())) {
          logger.warn("Empty consumer offset map from instance {}. Ignoring the result", instance);
          return;
        }

        KafkaConsumerOffsetsResponse reducedResponse = result.computeIfAbsent(response.getConsumerGroupId(),
            k -> new KafkaConsumerOffsetsResponse(response.getConsumerGroupId()));

        Map<String, Map<Integer, Long>> consumerOffsets = response.getConsumerOffsets();
        consumerOffsets.forEach((topic, partitionOffsets) -> {
          Map<String, Map<Integer, Long>> reducedConsumerOffsets = reducedResponse.getConsumerOffsets();
          Map<Integer, Long> reducedPartitionOffsets = reducedConsumerOffsets.computeIfAbsent(topic, k -> new HashMap<>());
          reducedPartitionOffsets.putAll(partitionOffsets);
        });
      });
    });

    return JsonUtils.toJson(result.values());
  }
}
