/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Util class for logging-related methods
 */
public class LogUtils {
  private static final Logger LOG = LoggerFactory.getLogger(LogUtils.class.getName());
  private static final double BUFFER_1KB =  1024;

  private static void printNumberRange(StringBuilder stringBuilder, int start, int tail) {
    if (start == tail) {
      stringBuilder.append(start);
    } else {
      stringBuilder.append(start).append("-").append(tail);
    }
  }

  /**
   * Shortening the list of integers by merging consecutive numbers together. e.g.
   * [1, 2, 4, 5, 6] -> [1-2, 4-6]
   * @param list list of integers to generate logging String for
   * @return compacted String that merges consecutive numbers
   */
  public static String logNumberArrayInRange(List<Integer> list) {
    if (list == null || list.isEmpty()) {
      return "[]";
    }
    try {
      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append("[");
      List<Integer> copiedList = new ArrayList<>(list);
      copiedList.sort(Integer::compareTo);
      int curStart = copiedList.get(0);
      int curTail = curStart;
      for (int i = 1; i < copiedList.size(); i++) {
        int num = copiedList.get(i);
        if (num <= curTail + 1) {
          curTail = num;
        } else {
          printNumberRange(stringBuilder, curStart, curTail);
          stringBuilder.append(", ");
          curStart = num;
          curTail = num;
        }
      }
      printNumberRange(stringBuilder, curStart, curTail);
      stringBuilder.append("]");
      return stringBuilder.toString();
    } catch (Exception e) {
      LOG.error("Failed to generate string for the int list in range", e);
      return list.toString();
    }
  }

  /**
   * Shortening the list of topic-partition mappings by merging partitions of the same topic together. e.g.
   * topic1-0, topic1-1, topic2-0 -> topic1:[0-1], topic2:[0]
   * @param partitions list of strings to generate logging string for
   * @return compacted String that merges partitions per topic
   */
  public static String logSummarizedTopicPartitionsMapping(List<String> partitions) {
    if (partitions == null || partitions.isEmpty()) {
      return "[]";
    }
    final Map<String, List<Integer>> topicPartitionsMap;
    try {
      topicPartitionsMap = partitions.stream()
          .map(TopicPartitionUtil::createTopicPartition)
          .collect(Collectors.groupingBy(TopicPartition::topic,
              Collectors.mapping(TopicPartition::partition, Collectors.toList())));
    } catch (NumberFormatException e) {
      LOG.error(e.getMessage());
      return String.join(",", partitions);
    }
    return topicPartitionsMap.keySet()
        .stream()
        .map(topicName -> new StringBuilder(topicName).append(":")
            .append(logNumberArrayInRange(topicPartitionsMap.get(topicName))))
        .collect(Collectors.joining(", "));
  }

  /**
   * prints one log line for each string smaller than size limit
   * @param  message
   */
  public static void logStringsUnderSizeLimit(Logger log, String message, String contextPrefix, int part, double sizeLimit) {
    int bufferAdjustedSizeLimit = (int) (sizeLimit - BUFFER_1KB);
    if (isLessThanSizeLimit(message, sizeLimit)) {
      if (part == 1) {
        log.info("{}={}", contextPrefix,  message);
      } else {
        log.info("{} (part {})={}", contextPrefix, part,  message);
      }
    } else {
      log.info("{} (part {})={}", contextPrefix, part, message.substring(0, bufferAdjustedSizeLimit));
      logStringsUnderSizeLimit(log, message.substring(bufferAdjustedSizeLimit), contextPrefix, part + 1, sizeLimit);
    }
  }

  /**
   * helper function to check if string size is less than size limit
   * @param message string to check size of
   * @return true if message is less than size limit
   */
  private static boolean isLessThanSizeLimit(String message, double sizeLimit) {
    double sizeInMB = getStringSizeInBytes(message);
    return sizeInMB + BUFFER_1KB < sizeLimit;
  }

  /**
   * helper function to get the size of a string (default charset UTF) in bytes
   * @param message string to measure
   * @return size of message in bytes
   */
  private static double getStringSizeInBytes(String message) {
    if (Objects.nonNull(message) && !message.isEmpty()) {
      return (message.getBytes(StandardCharsets.UTF_8).length);
    }
    return 0;
  }
}
