/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.common.zk.ZkClient;

/**
 * Brooklin {@link Coordinator} configuration properties
 */
public final class CoordinatorConfig {
  private static final String PREFIX = "brooklin.server.coordinator.";
  public static final String CONFIG_DEFAULT_TRANSPORT_PROVIDER = PREFIX + "defaultTransportProviderName";
  public static final String CONFIG_CLUSTER = PREFIX + "cluster";
  public static final String CONFIG_ZK_ADDRESS = PREFIX + "zkAddress";
  public static final String CONFIG_ZK_SESSION_TIMEOUT = PREFIX + "zkSessionTimeout";
  public static final String CONFIG_ZK_CONNECTION_TIMEOUT = PREFIX + "zkConnectionTimeout";
  public static final String CONFIG_RETRY_INTERVAL = PREFIX + "retryIntervalMs";
  public static final String CONFIG_HEARTBEAT_PERIOD_MS = PREFIX + "heartbeatPeriodMs";
  public static final String CONFIG_ZK_CLEANUP_ORPHAN_CONNECTOR_TASK = PREFIX + "zkCleanUpOrphanConnectorTask";
  // This config is temporary. If this config is enabled, on zookeeper session expiry, the node will be
  // brought down. Currently, session expiry is not handled correctly which either cause multiple zk sessions
  // or no zk session on session expiry. Its better to bring down the node rather than run in a fatal state.
  // TODO: Remove this config and add graceful handling of zk expiry and recover zookeeper and coordinator states.
  public static final String CONFIG_EXIT_ON_ZK_SESSION_EXPIRY = PREFIX + "exitOnZkSessionExpiry";

  private final String _cluster;
  private final String _zkAddress;
  private final int _zkSessionTimeout;
  private final int _zkConnectionTimeout;
  private final Properties _config;
  private final VerifiableProperties _properties;
  private final int _retryIntervalMs;
  private final long _heartbeatPeriodMs;
  private final String _defaultTransportProviderName;
  private final boolean _zkCleanUpOrphanConnectorTask;
  private final boolean _exitOnZkSessionExpiry;

  /**
   * Construct an instance of CoordinatorConfig
   * @param config configuration properties to load
   * @throws IllegalArgumentException if any required config property is missing
   */
  public CoordinatorConfig(Properties config) {
    _config = config;
    _properties = new VerifiableProperties(config);
    _cluster = _properties.getString(CONFIG_CLUSTER);
    _zkAddress = _properties.getString(CONFIG_ZK_ADDRESS);
    _zkSessionTimeout = _properties.getInt(CONFIG_ZK_SESSION_TIMEOUT, ZkClient.DEFAULT_SESSION_TIMEOUT);
    _zkConnectionTimeout = _properties.getInt(CONFIG_ZK_CONNECTION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
    _retryIntervalMs = _properties.getInt(CONFIG_RETRY_INTERVAL, 1000 /* 1 second */);
    _heartbeatPeriodMs = _properties.getLong(CONFIG_HEARTBEAT_PERIOD_MS, Duration.ofMinutes(1).toMillis());
    _defaultTransportProviderName = _properties.getString(CONFIG_DEFAULT_TRANSPORT_PROVIDER, "");
    _zkCleanUpOrphanConnectorTask = _properties.getBoolean(CONFIG_ZK_CLEANUP_ORPHAN_CONNECTOR_TASK, false);
    _exitOnZkSessionExpiry = _properties.getBoolean(CONFIG_EXIT_ON_ZK_SESSION_EXPIRY, false);
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

  public int getRetryIntervalMs() {
    return _retryIntervalMs;
  }

  public String getDefaultTransportProviderName() {
    return _defaultTransportProviderName;
  }

  public long getHeartbeatPeriodMs() {
    return _heartbeatPeriodMs;
  }

  public boolean getZkCleanUpOrphanConnectorTask() {
    return _zkCleanUpOrphanConnectorTask;
  }

  public boolean getExitOnZkSessionExpiry() {
    return _exitOnZkSessionExpiry;
  }

}
