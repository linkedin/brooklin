/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */

package com.linkedin.datastream.common;

import org.apache.kafka.common.TopicPartition;

/**
 * Utility class for converting String to TopicPartitions
 */
public class TopicPartitionUtil {

  /**
   * Create a topic partition instance from String
   */
  public static TopicPartition createTopicPartition(String str) {
    int i = str.lastIndexOf("-");
    int partition = Integer.parseInt(str.substring(i + 1));
    String topic = str.substring(0, i);
    return new TopicPartition(topic, partition);
  }
}
