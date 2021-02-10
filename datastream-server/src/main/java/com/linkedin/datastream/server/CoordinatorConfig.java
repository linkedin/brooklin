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
  // debounce timer is used to postpone the operations by a certain time to handle zookeeper session expiry.
  public static final String CONFIG_DEBOUNCE_TIMER_MS = PREFIX + "debounceTimerMs";
  public static final String CONFIG_RETRY_INTERVAL = PREFIX + "retryIntervalMs";
  public static final String CONFIG_HEARTBEAT_PERIOD_MS = PREFIX + "heartbeatPeriodMs";
  public static final String CONFIG_ZK_CLEANUP_ORPHAN_CONNECTOR_TASK = PREFIX + "zkCleanUpOrphanConnectorTask";
  public static final String CONFIG_ZK_CLEANUP_ORPHAN_CONNECTOR_TASK_LOCK = PREFIX + "zkCleanUpOrphanConnectorTaskLock";
  public static final String CONFIG_MAX_DATASTREAM_TASKS_PER_INSTANCE = PREFIX + "maxDatastreamTasksPerInstance";
  public static final String CONFIG_PERFORM_PRE_ASSIGNMENT_CLEANUP = PREFIX + "performPreAssignmentCleanup";

  private final String _cluster;
  private final String _zkAddress;
  private final int _zkSessionTimeout;
  private final int _zkConnectionTimeout;
  private final Properties _config;
  private final VerifiableProperties _properties;
  private final int _retryIntervalMs;
  private final long _debounceTimerMs;
  private final long _heartbeatPeriodMs;
  private final String _defaultTransportProviderName;
  private final boolean _zkCleanUpOrphanConnectorTask;
  private final boolean _zkCleanUpOrphanConnectorTaskLock;
  private final int _maxDatastreamTasksPerInstance;
  private final boolean _performPreAssignmentCleanup;

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
    _debounceTimerMs = _properties.getLong(CONFIG_DEBOUNCE_TIMER_MS, Duration.ofSeconds(120).toMillis());
    _defaultTransportProviderName = _properties.getString(CONFIG_DEFAULT_TRANSPORT_PROVIDER, "");
    _zkCleanUpOrphanConnectorTask = _properties.getBoolean(CONFIG_ZK_CLEANUP_ORPHAN_CONNECTOR_TASK, false);
    _zkCleanUpOrphanConnectorTaskLock = _properties.getBoolean(CONFIG_ZK_CLEANUP_ORPHAN_CONNECTOR_TASK_LOCK, false);
    _maxDatastreamTasksPerInstance = _properties.getInt(CONFIG_MAX_DATASTREAM_TASKS_PER_INSTANCE, 0);
    _performPreAssignmentCleanup = _properties.getBoolean(CONFIG_PERFORM_PRE_ASSIGNMENT_CLEANUP, false);
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

  public boolean getZkCleanUpOrphanConnectorTaskLock() {
    return _zkCleanUpOrphanConnectorTaskLock;
  }

  public int getMaxDatastreamTasksPerInstance() {
    return _maxDatastreamTasksPerInstance;
  }

  public boolean getPerformPreAssignmentCleanup() {
    return _performPreAssignmentCleanup;
  }

  public long getDebounceTimerMs() {
    return _debounceTimerMs;
  }
}
