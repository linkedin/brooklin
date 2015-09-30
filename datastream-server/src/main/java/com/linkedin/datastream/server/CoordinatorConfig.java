package com.linkedin.datastream.server;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.zk.ZkClient;


public class CoordinatorConfig {
  private final String _cluster;
  private final String _zkAddress;
  private final int _zkSessionTimeout;
  private final int _zkConnectionTimeout;
  private static final String PREFIX = "datastream.server.coordinator.";

  public CoordinatorConfig(VerifiableProperties properties) {
    _cluster = properties.getString(PREFIX + "cluster");
    _zkAddress = properties.getString(PREFIX + "zkAddress");
    _zkSessionTimeout = properties.getInt(PREFIX + "zkSessionTimeout", ZkClient.DEFAULT_SESSION_TIMEOUT);
    _zkConnectionTimeout = properties.getInt(PREFIX + "zkConnectionTimeout", ZkClient.DEFAULT_CONNECTION_TIMEOUT);
  }

  public final String getCluster() {
    return _cluster;
  }

  public final String getZkAddress() {
    return _zkAddress;
  }

  public final int getZkSessionTimeout() {
    return _zkSessionTimeout;
  }

  public final int getZkConnectionTimeout() {
    return _zkConnectionTimeout;
  }
}
