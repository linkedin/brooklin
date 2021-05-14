/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * Performs partition assignment based on partition throughput information
 */
public class LoadBasedPartitionAssigner {
  /**
   * Get partition assignment
   * @param throughputInfo Per-partition throughput information
   * @param assignedPartitions List of assigned partitions
   * @param unassignedPartitions List of unassigned partitions
   * @param taskCount Task count
   * @return Partition assignment
   */
  public Map<String, Set<DatastreamTask>> assignPartitions(ClusterThroughputInfo throughputInfo,
      List<String> assignedPartitions, List<String> unassignedPartitions, int taskCount) {
    throw new NotImplementedException();
  }
}
