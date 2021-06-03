/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.PartitionThroughputInfo;


/**
 * Estimates the minimum number of tasks for a datastream based on per-partition throughput information
 * <p>
 *   The reason for having a separate class for task count estimation is the intent to refactor other task count
 *   estimation mechanisms (e.g. elastic task count estimation) and make them all implement the same interface.
 * </p>
 */
public class LoadBasedTaskCountEstimator {
  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedTaskCountEstimator.class.getName());
  // TODO Move these to config class
  private static final int BYTES_IN_KB_RATE_DEFAULT = 5;
  private static final int MESSAGES_IN_RATE_DEFAULT = 5;

  private final int _taskCapacityMBps;
  private final int _taskCapacityUtilizationPct;

  /**
   * Creates an instance of {@link LoadBasedTaskCountEstimator}
   * @param taskCapacityMBps Task capacity in MB/sec
   * @param taskCapacityUtilizationPct Task capacity utilization percentage
   */
  public LoadBasedTaskCountEstimator(int taskCapacityMBps, int taskCapacityUtilizationPct) {
    _taskCapacityMBps = taskCapacityMBps;
    _taskCapacityUtilizationPct = taskCapacityUtilizationPct;
  }

  /**
   * Gets the estimated number of tasks based on per-partition throughput information.
   * NOTE: This does not take into account numPartitionsPerTask configuration
   * @param throughputInfo Per-partition throughput information
   * @param assignedPartitions The list of assigned partitions
   * @param unassignedPartitions The list of unassigned partitions
   * @return The estimated number of tasks
   */
  public int getTaskCount(ClusterThroughputInfo throughputInfo, List<String> assignedPartitions,
      List<String> unassignedPartitions) {
    Validate.notNull(throughputInfo, "null throughputInfo");
    Validate.notNull(assignedPartitions, "null assignedPartitions");
    Validate.notNull(unassignedPartitions, "null unassignedPartitions");
    LOG.info("Assigned partitions: {}", assignedPartitions);
    LOG.info("Unassigned partitions: {}", unassignedPartitions);

    Map<String, PartitionThroughputInfo> throughputMap = throughputInfo.getPartitionInfoMap();
    Set<String> allPartitions = new HashSet<>(assignedPartitions);
    allPartitions.addAll(unassignedPartitions);

    PartitionThroughputInfo defaultThroughputInfo = new PartitionThroughputInfo(BYTES_IN_KB_RATE_DEFAULT,
        MESSAGES_IN_RATE_DEFAULT, "");
    // total throughput in KB/sec
    int totalThroughput = allPartitions.stream()
        .map(p -> throughputMap.getOrDefault(p, defaultThroughputInfo))
        .mapToInt(PartitionThroughputInfo::getBytesInKBRate)
        .sum();
    LOG.info("Total throughput in all {} partitions: {}KB/sec", allPartitions.size(), totalThroughput);

    double taskCapacityUtilizationCoefficient = _taskCapacityUtilizationPct / 100.0;
    int taskCountEstimate = (int) Math.ceil((double) totalThroughput /
        (_taskCapacityMBps * 1024 * taskCapacityUtilizationCoefficient));
    taskCountEstimate = Math.min(allPartitions.size(), taskCountEstimate);
    LOG.info("Estimated number of tasks required to handle the throughput: {}", taskCountEstimate);
    return taskCountEstimate;
  }
}
