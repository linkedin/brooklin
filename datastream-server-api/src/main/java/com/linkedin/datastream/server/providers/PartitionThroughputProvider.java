/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.util.Map;

import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroup;


/**
 * Abstraction that provides topic partition throughput information. Used by load-based assignment strategies to do
 * partition assignment based on throughput
 */
public interface PartitionThroughputProvider {

  /**
   * Retrieves per-partition throughput information for the given cluster
   * @param clusterName Name of the cluster
   * @return Throughput information for the requested cluster
   */
  ClusterThroughputInfo getThroughputInfo(String clusterName);

  /**
   * Retrieves per-partition throughput information for the given datastream group
   * @param datastreamGroup Datastream group
   * @return Throughput information for the provided datastream group
   */
  ClusterThroughputInfo getThroughputInfo(DatastreamGroup datastreamGroup);

  /**
   * Retrieves per-partition throughput information for all clusters
   * @return A map, where keys are cluster names and values are throughput information for the cluster
   */
  Map<String, ClusterThroughputInfo> getThroughputInfo();
}
