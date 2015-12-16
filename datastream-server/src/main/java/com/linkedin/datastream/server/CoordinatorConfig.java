package com.linkedin.datastream.server;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.zk.ZkClient;

import java.util.Properties;


public final class CoordinatorConfig {
  private final String _cluster;
  private final String _zkAddress;
  private final int _zkSessionTimeout;
  private final int _zkConnectionTimeout;
  private final Properties _config;
  private final VerifiableProperties _properties;
  private final int _retryIntervalMS;
  private final String _transportProviderFactory;
  private final boolean _reuseExistingDestination;

  private static final String PREFIX = "datastream.server.coordinator.";
  public static final String CONFIG_CLUSTER = PREFIX + "cluster";
  public static final String CONFIG_ZK_ADDRESS = PREFIX + "zkAddress";
  public static final String CONFIG_ZK_SESSION_TIMEOUT = PREFIX + "zkSessionTimeout";
  public static final String CONFIG_ZK_CONNECTION_TIMEOUT = PREFIX + "zkConnectionTimeout";
  public static final String CONFIG_RETRY_INTERVAL = PREFIX + "retryIntervalMS";
  public static final String CONFIG_TRANSPORT_PROVIDER_FACTORY = PREFIX + "transportProviderFactory";
  public static final String CONFIG_SCHEMA_REGISTRY_PROVIDER_FACTORY = "schemaRegistryProviderFactory";
  public static final String CONFIG_REUSE_EXISTING_DESTINATION = "reuseExistingDestination";
  private final String _schemaRegistryProviderFactory;

  public CoordinatorConfig(Properties config) {
    _config = config;
    _properties = new VerifiableProperties(config);
    _cluster = _properties.getString(CONFIG_CLUSTER);
    _zkAddress = _properties.getString(CONFIG_ZK_ADDRESS);
    _zkSessionTimeout = _properties.getInt(CONFIG_ZK_SESSION_TIMEOUT, ZkClient.DEFAULT_SESSION_TIMEOUT);
    _zkConnectionTimeout = _properties.getInt(CONFIG_ZK_CONNECTION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    _retryIntervalMS = _properties.getInt(CONFIG_RETRY_INTERVAL, 1000 /* 1 second */);
    _transportProviderFactory = _properties.getString(CONFIG_TRANSPORT_PROVIDER_FACTORY);
    _schemaRegistryProviderFactory = _properties.getString(CONFIG_SCHEMA_REGISTRY_PROVIDER_FACTORY, null);
    _reuseExistingDestination = _properties.getBoolean(CONFIG_REUSE_EXISTING_DESTINATION, true);
  }

  public Properties getConfigProperties() {
    return _config;
  }

  public String getCluster() {
    return _cluster;
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public int getZkSessionTimeout() {
    return _zkSessionTimeout;
  }

  public int getZkConnectionTimeout() {
    return _zkConnectionTimeout;
  }

  public int getRetryIntervalMS() {
    return _retryIntervalMS;
  }

  public String getTransportProviderFactory() {
    return _transportProviderFactory;
  }

  public String getSchemaRegistryProviderFactory() {
    return _schemaRegistryProviderFactory;
  }

  public boolean isReuseExistingDestination() {
    return _reuseExistingDestination;
  }
}
