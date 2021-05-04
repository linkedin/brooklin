/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.linkedin.datastream.server.ClusterThroughputInfo;

/**
 * Estimates the minimum number of tasks for a datastream based on per-partition throughput information.
 */
public class LoadBasedTaskCountEstimator {
  /**
   * Gets the estimated number of min tasks
   * @param throughputInfo Per-partition throughput information
   * @param assignedPartitions The list of assigned partitions
   * @param unassignedPartitions The list of unassigned partitions
   * @return The estimated number of tasks
   */
  public int getTaskCount(ClusterThroughputInfo throughputInfo, List<String> assignedPartitions,
      List<String> unassignedPartitions) {
    throw new NotImplementedException();
  }
}
