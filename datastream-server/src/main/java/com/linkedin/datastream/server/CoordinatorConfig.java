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
  public static final String CONFIG_REINIT_ON_NEW_ZK_SESSION = PREFIX + "reinitOnNewZKSession";
  public static final String CONFIG_MAX_ASSIGNMENT_RETRY_COUNT = PREFIX + "maxAssignmentRetryCount";
  public static final String CONFIG_ENABLE_ASSIGNMENT_TOKENS = PREFIX + "enableAssignmentTokens";

  // how long should the leader poll the zookeeper to confirm that stopping datastreams have stopped before giving up
  public static final String CONFIG_STOP_PROPAGATION_TIMEOUT_MS = PREFIX + "stopPropagationTimeoutMs";
  // how long should the coordinator wait for a connector's tasks to stop before giving up
  public static final String CONFIG_TASK_STOP_CHECK_TIMEOUT_MS = PREFIX + "taskStopCheckTimeoutMs";
  // how long should the coordinator wait in between two consecutive calls to check if a connector's tasks have stopped
  public static final String CONFIG_TASK_STOP_CHECK_RETRY_PERIOD_MS = PREFIX + "taskStopCheckRetryPeriodMs";
  // should the coordinator declare a stream stopped if there's no confirmation from at least one follower
  public static final String CONFIG_FORCE_STOP_STREAMS_ON_FAILURE = PREFIX + "forceStopStreamsOnFailure";
  // how long should the coordinator attempt to mark stopping datastreams stopped before giving up
  public static final String CONFIG_MARK_DATASTREAMS_STOPPED_TIMEOUT_MS = PREFIX + "markDatastreamsStoppedTimeoutMs";
  // how long should the coordinator wait between attempts to mark stopping datastreams stopped
  public static final String CONFIG_MARK_DATASTREAMS_STOPPED_RETRY_PERIOD_MS = PREFIX + "markDatastreamsStoppedRetryPeriodMs";

  public static final String CONFIG_ENABLE_THROUGHPUT_VIOLATING_TOPICS_HANDLING = PREFIX + "enableThroughputViolatingTopicsHandling";

  public static final String CONFIG_OVERRIDE_DATASTREAM_UPDATE_CHECKS = PREFIX + "overrideDatastreamUpdateChecks";

  public static final int DEFAULT_MAX_ASSIGNMENT_RETRY_COUNT = 100;
  public static final long DEFAULT_STOP_PROPAGATION_TIMEOUT_MS = 60 * 1000;
  public static final long DEFAULT_TASK_STOP_CHECK_TIMEOUT_MS = 60 * 1000;
  public static final long DEFAULT_TASK_STOP_CHECK_RETRY_PERIOD_MS = 10 * 1000;
  public static final int DEFAULT_MARK_DATASTREMS_STOPPED_TIMEOUT_MS = 60 * 1000;
  public static final int DEFAULT_MARK_DATASTREMS_STOPPED_RETRY_PERIOD_MS = 10 * 1000;

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
  private final boolean _reinitOnNewZkSession;
  private final int _maxAssignmentRetryCount;
  private final boolean _enableAssignmentTokens;
  private final long _stopPropagationTimeoutMs;
  private final long _taskStopCheckTimeoutMs;
  private final long _taskStopCheckRetryPeriodMs;
  private final boolean _forceStopStreamsOnFailure;
  private final long _markDatastreamsStoppedTimeoutMs;
  private final long _markDatastreamsStoppedRetryPeriodMs;
  private final boolean _enableThroughputViolatingTopicsHandling;
  private final boolean _overrideDatastreamUpdateChecks;


  /**
   * Construct an instance of CoordinatorConfig
   * @param config configuration properties to load
   * @throws IllegalArgumentException if any required config property is missing
   *
   * TODO : Add unit tests for all coordinator config properties
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
    _reinitOnNewZkSession = _properties.getBoolean(CONFIG_REINIT_ON_NEW_ZK_SESSION, false);
    _maxAssignmentRetryCount = _properties.getInt(CONFIG_MAX_ASSIGNMENT_RETRY_COUNT, DEFAULT_MAX_ASSIGNMENT_RETRY_COUNT);
    _enableAssignmentTokens = _properties.getBoolean(CONFIG_ENABLE_ASSIGNMENT_TOKENS, false);
    _stopPropagationTimeoutMs = _properties.getLong(CONFIG_STOP_PROPAGATION_TIMEOUT_MS, DEFAULT_STOP_PROPAGATION_TIMEOUT_MS);
    _taskStopCheckTimeoutMs = _properties.getLong(CONFIG_TASK_STOP_CHECK_TIMEOUT_MS, DEFAULT_TASK_STOP_CHECK_TIMEOUT_MS);
    _taskStopCheckRetryPeriodMs = _properties.getLong(CONFIG_TASK_STOP_CHECK_RETRY_PERIOD_MS,
        DEFAULT_TASK_STOP_CHECK_RETRY_PERIOD_MS);
    _forceStopStreamsOnFailure = _properties.getBoolean(CONFIG_FORCE_STOP_STREAMS_ON_FAILURE, false);
    _markDatastreamsStoppedTimeoutMs = _properties.getLong(CONFIG_MARK_DATASTREAMS_STOPPED_TIMEOUT_MS,
        DEFAULT_MARK_DATASTREMS_STOPPED_TIMEOUT_MS);
    _markDatastreamsStoppedRetryPeriodMs = _properties.getLong(CONFIG_MARK_DATASTREAMS_STOPPED_RETRY_PERIOD_MS,
        DEFAULT_MARK_DATASTREMS_STOPPED_RETRY_PERIOD_MS);
    _enableThroughputViolatingTopicsHandling = _properties.getBoolean(
        CONFIG_ENABLE_THROUGHPUT_VIOLATING_TOPICS_HANDLING, false);
    _overrideDatastreamUpdateChecks = _properties.getBoolean(
        CONFIG_OVERRIDE_DATASTREAM_UPDATE_CHECKS, false);
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

  public boolean getReinitOnNewZkSession() {
    return _reinitOnNewZkSession;
  }

  public int getMaxAssignmentRetryCount() {
    return _maxAssignmentRetryCount;
  }

  public boolean getEnableThroughputViolatingTopicsHandling() {
    return _enableThroughputViolatingTopicsHandling;
  }

  // Configuration properties for Assignment Tokens Feature

  public boolean getEnableAssignmentTokens() {
    return _enableAssignmentTokens;
  }

  public long getTaskStopCheckTimeoutMs() {
    return _taskStopCheckTimeoutMs;
  }

  public long getTaskStopCheckRetryPeriodMs() {
    return _taskStopCheckRetryPeriodMs;
  }

  public long getStopPropagationTimeoutMs() {
    return _stopPropagationTimeoutMs;
  }

  public boolean getForceStopStreamsOnFailure() {
    return _forceStopStreamsOnFailure;
  }

  public long getMarkDatastreamsStoppedTimeoutMs() {
    return _markDatastreamsStoppedTimeoutMs;
  }

  public long getMarkDatastreamsStoppedRetryPeriodMs() {
    return _markDatastreamsStoppedRetryPeriodMs;
  }

  public boolean getOverrideDatastreamUpdateChecks() {
    return _overrideDatastreamUpdateChecks;
  }
}
