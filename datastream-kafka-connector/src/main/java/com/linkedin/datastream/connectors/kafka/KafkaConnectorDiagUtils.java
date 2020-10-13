/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.connectors.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
    List<KafkaTopicPartitionStatsResponse> result = new ArrayList<>();

    // flatten responses from all hosts
    responses.forEach((instance, json) -> {
      List<KafkaTopicPartitionStatsResponse> responseList;
      try {
        responseList = JsonUtils.fromJson(json, new TypeReference<List<KafkaTopicPartitionStatsResponse>>() {
        });
      } catch (Exception e) {
        logger.error("Invalid response {} from instance {}", json, instance);
        return;
      }
      result.addAll(responseList);
    });

    return JsonUtils.toJson(result);
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
        if (response.getConsumedOffsets() == null || response.getConsumedOffsets().isEmpty()) {
          logger.warn("Empty consumer offset map from instance {}. Ignoring the result", instance);
        } else if (StringUtils.isBlank(response.getConsumerGroupId())) {
          logger.warn("Invalid consumer group id from instance {}, Ignoring the result", instance);
        } else {
          KafkaConsumerOffsetsResponse reducedResponse = result.computeIfAbsent(response.getConsumerGroupId(),
              k -> new KafkaConsumerOffsetsResponse(response.getConsumerGroupId()));

          Map<String, Map<Integer, Long>> consumedOffsets = response.getConsumedOffsets();
          consumedOffsets.forEach((topic, partitionOffsets) -> {
            Map<String, Map<Integer, Long>> reducedConsumedOffsets = reducedResponse.getConsumedOffsets();
            Map<Integer, Long> reducedPartitionOffsets = reducedConsumedOffsets.computeIfAbsent(topic, k -> new HashMap<>());
            reducedPartitionOffsets.putAll(partitionOffsets);
          });

          Map<String, Map<Integer, Long>> committedOffsets = response.getCommittedOffsets();
          committedOffsets.forEach((topic, partitionOffsets) -> {
            Map<String, Map<Integer, Long>> reducedCommittedOffsets = reducedResponse.getCommittedOffsets();
            Map<Integer, Long> reducedPartitionOffsets = reducedCommittedOffsets.computeIfAbsent(topic, k -> new HashMap<>());
            reducedPartitionOffsets.putAll(partitionOffsets);
          });
        }
      });
    });

    return JsonUtils.toJson(result.values());
  }
}
