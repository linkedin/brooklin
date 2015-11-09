package com.linkedin.datastream.server;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.zk.ZkClient;


public class CoordinatorConfig {
  private final String _cluster;
  private final String _zkAddress;
  private final int _zkSessionTimeout;
  private final int _zkConnectionTimeout;
  private final VerifiableProperties _config;

  private static final String PREFIX = "datastream.server.coordinator.";
  public static final String CONFIG_CLUSTER = PREFIX + "cluster";
  public static final String CONFIG_ZK_ADDRESS = PREFIX + "zkAddress";
  public static final String CONFIG_ZK_SESSION_TIMEOUT = PREFIX + "zkSessionTimeout";
  public static final String CONFIG_ZK_CONNECTION_TIMEOUT = PREFIX + "zkConnectionTimeout";

  public CoordinatorConfig(VerifiableProperties properties) {
    _config = properties;
    _cluster = properties.getString(CONFIG_CLUSTER);
    _zkAddress = properties.getString(CONFIG_ZK_ADDRESS);
    _zkSessionTimeout = properties.getInt(CONFIG_ZK_SESSION_TIMEOUT, ZkClient.DEFAULT_SESSION_TIMEOUT);
    _zkConnectionTimeout = properties.getInt(CONFIG_ZK_CONNECTION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
  }

  public VerifiableProperties getConfigProperties() {
    return _config;
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
