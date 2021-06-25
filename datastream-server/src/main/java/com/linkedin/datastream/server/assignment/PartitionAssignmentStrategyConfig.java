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
import static com.linkedin.datastream.server.assignment.StickyMulticastStrategyFactory.DEFAULT_IMBALANCE_THRESHOLD;


/**
 * Configuration properties for {@link StickyPartitionAssignmentStrategy} and its extensions
 */
public class PartitionAssignmentStrategyConfig {
  public static final String CFG_MAX_PARTITION_PER_TASK = "maxPartitionsPerTask";
  public static final String CFG_PARTITIONS_PER_TASK = "partitionsPerTask";
  public static final String CFG_PARTITION_FULLNESS_THRESHOLD_PCT = "partitionFullnessThresholdPct";
  public static final String CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT = "enableElasticTaskAssignment";
  public static final String CFG_CLUSTER_NAME = "cluster";
  public static final String CFG_ZK_ADDRESS = "zkAddress";
  public static final String CFG_ZK_SESSION_TIMEOUT = "zkSessionTimeout";
  public static final String CFG_ZK_CONNECTION_TIMEOUT = "zkConnectionTimeout";

  public static final boolean DEFAULT_ENABLE_ELASTIC_TASK_ASSIGNMENT = false;
  public static final int DEFAULT_PARTITIONS_PER_TASK = 500;
  private static final int DEFAULT_PARTITION_FULLNESS_FACTOR_PCT = 75;

  private final Properties _config;
  private final Optional<Integer> _maxTasks;
  private final int _imbalanceThreshold;
  private final int _maxPartitions;
  private final int _partitionsPerTask;
  private final int _partitionFullnessThresholdPct;
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
    // Set to Optional.empty() if the value is 0
    _maxTasks = cfgMaxTasks > 0 ? Optional.of(cfgMaxTasks) : Optional.empty();
    int cfgImbalanceThreshold = props.getInt(CFG_IMBALANCE_THRESHOLD, DEFAULT_IMBALANCE_THRESHOLD);
    _imbalanceThreshold = cfgImbalanceThreshold >= 0 ? cfgImbalanceThreshold : DEFAULT_IMBALANCE_THRESHOLD;
    int cfgMaxPartitionsPerTask = props.getInt(CFG_MAX_PARTITION_PER_TASK, Integer.MAX_VALUE);
    _maxPartitions = cfgMaxPartitionsPerTask > 0 ? cfgMaxPartitionsPerTask : Integer.MAX_VALUE;

    int cfgPartitionsPerTask = props.getInt(CFG_PARTITIONS_PER_TASK, DEFAULT_PARTITIONS_PER_TASK);
    _partitionsPerTask = cfgPartitionsPerTask > 0 ? cfgPartitionsPerTask : DEFAULT_PARTITIONS_PER_TASK;
    _partitionFullnessThresholdPct = props.getIntInRange(CFG_PARTITION_FULLNESS_THRESHOLD_PCT,
        DEFAULT_PARTITION_FULLNESS_FACTOR_PCT, 0, 100);
    _enableElasticTaskAssignment = props.getBoolean(CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT, DEFAULT_ENABLE_ELASTIC_TASK_ASSIGNMENT);
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
  public int getImbalanceThreshold() {
    return _imbalanceThreshold;
  }

  /**
   * Gets max partitions
   * @return Max partitions
   */
  public int getMaxPartitions() {
    return _maxPartitions;
  }

  /**
   * Gets partitions per task
   * @return Partitions per task
   */
  public int getPartitionsPerTask() {
    return _partitionsPerTask;
  }

  /**
   * Gets partition fullness threshold percentage
   * @return Partition fullness threshold percentage
   */
  public int getPartitionFullnessThresholdPct() {
    return _partitionFullnessThresholdPct;
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
