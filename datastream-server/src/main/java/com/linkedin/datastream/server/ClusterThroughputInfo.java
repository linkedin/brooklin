package com.linkedin.datastream.server;

import java.util.HashMap;


/**
 * A structure that holds per-partition throughput information for a Kafka cluster/
 */
public class ClusterThroughputInfo {

  private String _clusterName;

  private HashMap<String, PartitionThroughputInfo> _partitionInfoMap;

  /**
   * Creates an instance of {@link ClusterThroughputInfo}
   * @param clusterName Cluster name
   * @param partitionInfoMap A map, where the key is the partition name, and the value is the
   *                         {@link PartitionThroughputInfo} for the partition.
   */
  public ClusterThroughputInfo(String clusterName, HashMap<String, PartitionThroughputInfo> partitionInfoMap) {
    _clusterName = clusterName;
    _partitionInfoMap = partitionInfoMap;
  }

  /**
   * Creates an instance of {@link ClusterThroughputInfo}
   */
  public ClusterThroughputInfo() { }

  /**
   * Gets the cluster name
   * @return Name of the cluster
   */
  public String getClusterName() {
    return _clusterName;
  }

  /**
   * Gets the partition information map for partitions in the cluster
   * @return A map, where the key is the partition name, and the value is a {@link PartitionThroughputInfo} for the
   * partition
   */
  public HashMap<String, PartitionThroughputInfo> getPartitionInfoMap() {
    return _partitionInfoMap;
  }

  /**
   * Sets the cluster name
   * @param clusterName Name of the cluster
   */
  public void setClusterName(String clusterName) {
    _clusterName = clusterName;
  }

  /**
   * Sets the partition information map.
   * @param partitionInfoMap A map, where the key is the partition name, and the value is a {@link PartitionThroughputInfo}
   *                         for the partition
   */
  public void setPartitionInfoMap(HashMap<String, PartitionThroughputInfo> partitionInfoMap) {
    _partitionInfoMap = _partitionInfoMap;
  }
}
