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
 */
public class LoadBasedTaskCountEstimator {

  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedTaskCountEstimator.class.getName());
  private final static int TASK_CAPACITY_MBPS_DEFAULT = 4;

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

    Map<String, PartitionThroughputInfo> throughputMap = throughputInfo.getPartitionInfoMap();

    Set<String> allPartitions = new HashSet<>(assignedPartitions);
    allPartitions.addAll(unassignedPartitions);

    // total throughput in KB/sec
    int totalThroughput = throughputMap.entrySet().stream().
        filter(e -> allPartitions.contains(e.getKey())).mapToInt(e -> e.getValue().getBytesInRate()).sum();
    LOG.debug("Total throughput in all partitions: {}KB/sec", totalThroughput);

    int taskCountEstimate = (int) Math.ceil((double) totalThroughput / (TASK_CAPACITY_MBPS_DEFAULT * 1024));
    LOG.debug("Estimated number of tasks required to handle the throughput: {}", taskCountEstimate);
    return taskCountEstimate;
  }
}
