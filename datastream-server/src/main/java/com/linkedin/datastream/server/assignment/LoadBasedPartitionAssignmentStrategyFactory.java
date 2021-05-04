/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Optional;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;
import com.linkedin.datastream.server.providers.NoOpPartitionThroughputProvider;
import com.linkedin.datastream.server.providers.PartitionThroughputProvider;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;
import static com.linkedin.datastream.server.assignment.StickyMulticastStrategyFactory.CFG_IMBALANCE_THRESHOLD;


/**
 * A factory for creating {@link LoadBasedPartitionAssignmentStrategy} instances
 */
public class LoadBasedPartitionAssignmentStrategyFactory implements AssignmentStrategyFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedPartitionAssignmentStrategyFactory.class.getName());

  public static final String CFG_MAX_PARTITION_PER_TASK = "maxPartitionsPerTask";
  public static final String CFG_PARTITIONS_PER_TASK = "partitionsPerTask";
  public static final String CFG_PARTITION_FULLNESS_THRESHOLD_PCT = "partitionFullnessThresholdPct";
  public static final String CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT = "enableElasticTaskAssignment";
  public static final String CFG_ZK_ADDRESS = "zkAddress";
  public static final String CFG_ZK_SESSION_TIMEOUT = "zkSessionTimeout";
  public static final String CFG_ZK_CONNECTION_TIMEOUT = "zkConnectionTimeout";
  public static final String CFG_CLUSTER_NAME = "cluster";

  public static final boolean DEFAULT_ENABLE_ELASTIC_TASK_ASSIGNMENT = false;
  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    VerifiableProperties props = new VerifiableProperties(assignmentStrategyProperties);
    int cfgMaxTasks = props.getInt(CFG_MAX_TASKS, 0);
    int cfgImbalanceThreshold = props.getInt(CFG_IMBALANCE_THRESHOLD, 0);
    int cfgMaxParitionsPerTask = props.getInt(CFG_MAX_PARTITION_PER_TASK, 0);
    // Set to Optional.empty() if the value is 0
    Optional<Integer> maxTasks = cfgMaxTasks > 0 ? Optional.of(cfgMaxTasks) : Optional.empty();
    Optional<Integer> imbalanceThreshold = cfgImbalanceThreshold > 0 ? Optional.of(cfgImbalanceThreshold)
        : Optional.empty();
    Optional<Integer> maxPartitions = cfgMaxParitionsPerTask > 0 ? Optional.of(cfgMaxParitionsPerTask) :
        Optional.empty();
    boolean enableElasticTaskAssignment = props.getBoolean(CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT,
        DEFAULT_ENABLE_ELASTIC_TASK_ASSIGNMENT);
    int cfgPartitionsPerTask = props.getInt(CFG_PARTITIONS_PER_TASK, 0);
    int cfgPartitionFullnessThresholdPct = props.getIntInRange(CFG_PARTITION_FULLNESS_THRESHOLD_PCT, 0, 0, 100);
    Optional<Integer> partitionsPerTask = cfgPartitionsPerTask > 0 ? Optional.of(cfgMaxParitionsPerTask) :
        Optional.empty();
    Optional<Integer> partitionFullnessThresholdPct = cfgPartitionFullnessThresholdPct > 0 ?
        Optional.of(cfgPartitionFullnessThresholdPct) : Optional.empty();
    String cluster = props.getString(CFG_CLUSTER_NAME, null);

    // Create the ZooKeeper Client
    Optional<ZkClient> zkClient = Optional.empty();
    String zkAddress = props.getString(CFG_ZK_ADDRESS, null);
    if (enableElasticTaskAssignment && StringUtils.isBlank(zkAddress)) {
      LOG.warn("Disabling elastic task assignment as zkAddress is not present or empty");
      enableElasticTaskAssignment = false;
    }

    if (enableElasticTaskAssignment) {
      int zkSessionTimeout = props.getInt(CFG_ZK_SESSION_TIMEOUT, ZkClient.DEFAULT_SESSION_TIMEOUT);
      int zkConnectionTimeout = props.getInt(CFG_ZK_CONNECTION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
      zkClient = Optional.of(new ZkClient(zkAddress, zkSessionTimeout, zkConnectionTimeout));
    }

    // TODO Plug in an actual throughput provider implementation
    PartitionThroughputProvider provider = new NoOpPartitionThroughputProvider();

    return new LoadBasedPartitionAssignmentStrategy(provider, null,
        maxTasks, imbalanceThreshold, maxPartitions, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessThresholdPct, zkClient, cluster);
  }
}
