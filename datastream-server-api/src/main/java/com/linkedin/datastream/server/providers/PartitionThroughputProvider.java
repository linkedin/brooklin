package com.linkedin.datastream.server.providers;

import com.linkedin.datastream.server.ClusterThroughputInfo;
import java.util.HashMap;


/**
 * Abstraction that provides topic partition throughput information. Used by load-based assignment strategies to do
 * partition assignment based on throughput.
 */
public interface PartitionThroughputProvider {

  /**
   * Retrieves per-partition throughput information for the given cluster.
   * @param clusterName Name of the cluster for which to retrieve throughput info.
   * @return Throughput information for the requested cluster.
   */
  ClusterThroughputInfo getThroughputInfo(String clusterName);

  /**
   * Retrieves per-partition throughput information for all clusters.
   * @return A map, where keys are cluster names and values are throughput information for the cluster.
   */
  HashMap<String, ClusterThroughputInfo> getThroughputInfo();
}
