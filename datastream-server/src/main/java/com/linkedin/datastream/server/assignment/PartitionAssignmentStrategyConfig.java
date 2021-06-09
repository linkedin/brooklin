/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Optional;
import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.common.zk.ZkClient;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;
import static com.linkedin.datastream.server.assignment.StickyMulticastStrategyFactory.CFG_IMBALANCE_THRESHOLD;


/**
 * Configuration properties for {@link StickyPartitionAssignmentStrategy} and its extensions
 */
public final class PartitionAssignmentStrategyConfig {
  public static final String CFG_MAX_PARTITION_PER_TASK = "maxPartitionsPerTask";
  public static final String CFG_PARTITIONS_PER_TASK = "partitionsPerTask";
  public static final String CFG_PARTITION_FULLNESS_THRESHOLD_PCT = "partitionFullnessThresholdPct";
  public static final String CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS = "throughputInfoFetchTimeoutMs";
  public static final String CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS = "throughputInfoFetchRetryPeriodMs";
  public static final String CFG_TASK_CAPACITY_MBPS = "taskCapacityMBps";
  public static final String CFG_TASK_CAPACITY_UTILIZATION_PCT = "taskCapacityUtilizationPct";
  public static final String CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT = "enableElasticTaskAssignment";
  public static final String CFG_CLUSTER_NAME = "cluster";
  public static final String CFG_ZK_ADDRESS = "zkAddress";
  public static final String CFG_ZK_SESSION_TIMEOUT = "zkSessionTimeout";
  public static final String CFG_ZK_CONNECTION_TIMEOUT = "zkConnectionTimeout";

  public static final int PARTITION_BYTES_IN_KB_RATE_DEFAULT = 5;
  public static final int PARTITION_MESSAGES_IN_RATE_DEFAULT = 5;

  public static final boolean DEFAULT_ENABLE_ELASTIC_TASK_ASSIGNMENT = false;

  private final Properties _config;
  private final Optional<Integer> _maxTasks;
  private final Optional<Integer> _imbalanceThreshold;
  private final Optional<Integer> _maxPartitions;
  private final Optional<Integer> _partitionsPerTask;
  private final Optional<Integer> _partitionFullnessThresholdPct;
  private final Optional<Integer> _taskCapacityMBps;
  private final Optional<Integer> _taskCapacityUtilizationPct;
  private final Optional<Integer> _throughputInfoFetchTimeoutMs;
  private final Optional<Integer> _throughputInfoFetchRetryPeriodMs;
  private final String _cluster;
  private final String _zkAddress;
  private final int _zkSessionTimeout;
  private final int _zkConnectionTimeout;
  private final boolean _enableElasticTaskAssignment;

  /**
   * Creates an instance of {@link PartitionAssignmentStrategyConfig}
   * @param config Config properties
   */
  public PartitionAssignmentStrategyConfig(Properties config) {
    _config = config;
    VerifiableProperties props = new VerifiableProperties(config);
    int cfgMaxTasks = props.getInt(CFG_MAX_TASKS, 0);
    int cfgImbalanceThreshold = props.getInt(CFG_IMBALANCE_THRESHOLD, 0);
    int cfgMaxPartitionsPerTask = props.getInt(CFG_MAX_PARTITION_PER_TASK, 0);
    int cfgPartitionsPerTask = props.getInt(CFG_PARTITIONS_PER_TASK, 0);
    int cfgPartitionFullnessThresholdPct = props.getIntInRange(CFG_PARTITION_FULLNESS_THRESHOLD_PCT, 0, 0, 100);
    int cfgTaskCapacityMBps = props.getInt(CFG_TASK_CAPACITY_MBPS, 0);
    int cfgTaskCapacityUtilizationPct = props.getIntInRange(CFG_TASK_CAPACITY_UTILIZATION_PCT, 0, 0, 100);
    int cfgThroughputInfoFetchTimeoutMs = props.getInt(CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS, 0);
    int cfgThroughputInfoFetchRetryPeriodMs = props.getInt(CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS, 0);

    // Set to Optional.empty() if the value is 0
    _maxTasks = cfgMaxTasks > 0 ? Optional.of(cfgMaxTasks) : Optional.empty();
    _imbalanceThreshold = cfgImbalanceThreshold > 0 ? Optional.of(cfgImbalanceThreshold) : Optional.empty();
    _maxPartitions = cfgMaxPartitionsPerTask > 0 ? Optional.of(cfgMaxPartitionsPerTask) : Optional.empty();
    _enableElasticTaskAssignment = props.getBoolean(CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT,
        DEFAULT_ENABLE_ELASTIC_TASK_ASSIGNMENT);
    _partitionsPerTask = cfgPartitionsPerTask > 0 ? Optional.of(cfgPartitionsPerTask) :
        Optional.empty();
    _partitionFullnessThresholdPct = cfgPartitionFullnessThresholdPct > 0 ?
        Optional.of(cfgPartitionFullnessThresholdPct) : Optional.empty();
    _taskCapacityMBps = cfgTaskCapacityMBps > 0 ? Optional.of(cfgTaskCapacityMBps) : Optional.empty();
    _taskCapacityUtilizationPct = cfgTaskCapacityUtilizationPct > 0 ? Optional.of(cfgTaskCapacityUtilizationPct) :
        Optional.empty();
    _throughputInfoFetchTimeoutMs = cfgThroughputInfoFetchTimeoutMs > 0 ?
        Optional.of(cfgThroughputInfoFetchTimeoutMs) : Optional.empty();
    _throughputInfoFetchRetryPeriodMs = cfgThroughputInfoFetchRetryPeriodMs > 0 ?
        Optional.of(cfgThroughputInfoFetchRetryPeriodMs) : Optional.empty();
    _cluster = props.getString(CFG_CLUSTER_NAME, null);
    _zkAddress = props.getString(CFG_ZK_ADDRESS, null);
    _zkSessionTimeout = props.getInt(CFG_ZK_SESSION_TIMEOUT, ZkClient.DEFAULT_SESSION_TIMEOUT);
    _zkConnectionTimeout = props.getInt(CFG_ZK_CONNECTION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
  }

  /**
   * Gets max tasks
   * @return Max tasks config value
   */
  public Optional<Integer> getMaxTasks() {
    return _maxTasks;
  }

  /**
   * Gets imbalance threshold
   * @return Imbalance threshold
   */
  public Optional<Integer> getImbalanceThreshold() {
    return _imbalanceThreshold;
  }

  /**
   * Gets max partitions
   * @return Max partitions
   */
  public Optional<Integer> getMaxPartitions() {
    return _maxPartitions;
  }

  /**
   * Gets partitions per task
   * @return Partitions per task
   */
  public Optional<Integer> getPartitionsPerTask() {
    return _partitionsPerTask;
  }

  /**
   * Gets partition fullness threshold percentage
   * @return Partition fullness threshold percentage
   */
  public Optional<Integer> getPartitionFullnessThresholdPct() {
    return _partitionFullnessThresholdPct;
  }

  /**
   * Gets task capacity measured in MB/sec
   * @return Task capacity in MB/sec
   */
  public Optional<Integer> getTaskCapacityMBps() {
    return _taskCapacityMBps;
  }

  /**
   * Gets task capacity utilization percentage
   * @return Task capacity utilization percentage
   */
  public Optional<Integer> getTaskCapacityUtilizationPct() {
    return _taskCapacityUtilizationPct;
  }

  /**
   * Gets throughput info fetch timeout in milliseconds
   * @return Throughput info fetch timeout in milliseconds
   */
  public Optional<Integer> getThroughputInfoFetchTimeoutMs() {
    return _throughputInfoFetchTimeoutMs;
  }

  /**
   * Gets the throughput info fetch retry period in milliseconds
   * @return Throughput info fetch retry period in milliseconds
   */
  public Optional<Integer> getThroughputInfoFetchRetryPeriodMs() {
    return _throughputInfoFetchRetryPeriodMs;
  }

  /**
   * Gets cluster
   * @return Cluster
   */
  public String getCluster() {
    return _cluster;
  }

  /**
   * Indicates whether elastic task assignment is enabled or not
   * @return A boolean value, that, if set to true, indicates that elastic task assignment is enabled
   */
  public boolean isElasticTaskAssignmentEnabled() {
    return _enableElasticTaskAssignment;
  }

  /**
   * Returns configuration properties
   * @return Configuration properties
   */
  public Properties getConfigProperties() {
    return _config;
  }

  /**
   * Returns ZooKeeper address
   * @return ZooKeeper address
   */
  public String getZkAddress() {
    return _zkAddress;
  }

  /**
   * Returns ZooKeeper session timeout
   * @return ZooKeeper session timeout
   */
  public int getZkSessionTimeout() {
    return _zkSessionTimeout;
  }

  /**
   * Returns ZooKeeper connection timeout
   * @return ZooKeeper connection timeout
   */
  public int getZkConnectionTimeout() {
    return _zkConnectionTimeout;
  }
}
