package com.linkedin.datastream.server;


/**
 * Abstraction that provides topic partition throughput information. Used by load-based assignment strategies to do
 * partition assignment based on throughput.
 */
public interface PartitionThroughputProvider {

  /**
   * Retrieves per-partition throughput information for the given cluster.
   * @param clusterName Name of the cluster for which to retrieve throughput info.
   * @return Throughput stats for the requested cluster.
   */
  ClusterThroughputInfo getThroughputInfo(String clusterName);
}
