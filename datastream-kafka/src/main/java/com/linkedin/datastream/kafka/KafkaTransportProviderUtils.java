/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;


public class KafkaTransportProviderUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTransportProviderUtils.class.getName());
  // Mapping destination URI to topic name
  private final static Map<String, String> URI_TOPICS = new ConcurrentHashMap<>();

  /**
   * Helper method to cache the lookup from destination URI to the Kafka topic names.
   * Please refer to {@link KafkaDestination} for the exact format of Kafka destination URI.
   */
  public static String getTopicName(Datastream datastream) {
    return getTopicName(datastream.getDestination().getConnectionString());
  }

  /**
   * Helper method to cache the lookup from destination URI to the Kafka topic names.
   * Please refer to {@link KafkaDestination} for the exact format of Kafka destination URI.
   */
  public static String getTopicName(String destinationUri) {
    if (URI_TOPICS.containsKey(destinationUri)) {
      return URI_TOPICS.get(destinationUri);
    } else {
      synchronized (URI_TOPICS) {
        if (URI_TOPICS.containsKey(destinationUri)) {
          return URI_TOPICS.get(destinationUri);
        }

        KafkaDestination destination = KafkaDestination.parse(destinationUri);
        String topicName = destination.getTopicName();
        URI_TOPICS.put(destinationUri, topicName);
        return topicName;
      }
    }
  }
}
