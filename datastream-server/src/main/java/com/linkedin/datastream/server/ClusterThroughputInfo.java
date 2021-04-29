package com.linkedin.datastream.server;

import java.util.HashMap;


public class ClusterThroughputInfo {

  private String _clusterName;

  private HashMap<String, PartitionThroughputInfo> _partitionInfoMap;

  public ClusterThroughputInfo(String clusterName, HashMap<String, PartitionThroughputInfo> partitionInfoMap) {
    _clusterName = clusterName;
    _partitionInfoMap = partitionInfoMap;
  }

  public ClusterThroughputInfo() { }

  public String getClusterName() {
    return _clusterName;
  }

  public HashMap<String, PartitionThroughputInfo> getPartitionInfoMap() {
    return _partitionInfoMap;
  }

  public void setClusterName(String clusterName) {
    _clusterName = clusterName;
  }

  public void setPartitionInfoMap(HashMap<String, PartitionThroughputInfo> partitionInfoMap) {
    _partitionInfoMap = _partitionInfoMap;
  }
}
