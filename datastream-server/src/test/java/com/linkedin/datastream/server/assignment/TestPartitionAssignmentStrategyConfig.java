/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Optional;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;
import static com.linkedin.datastream.server.assignment.PartitionAssignmentStrategyConfig.DEFAULT_PARTITIONS_PER_TASK;
import static com.linkedin.datastream.server.assignment.StickyMulticastStrategyFactory.CFG_IMBALANCE_THRESHOLD;
import static com.linkedin.datastream.server.assignment.StickyMulticastStrategyFactory.DEFAULT_IMBALANCE_THRESHOLD;


/**
 * Tests for {@link PartitionAssignmentStrategyConfig}
 */
public class TestPartitionAssignmentStrategyConfig {

  private static final String CFG_MAX_PARTITION_PER_TASK_VALUE = "10";
  private static final String CFG_IMBALANCE_THRESHOLD_VALUE = "90";
  private static final String CFG_MAX_TASKS_VALUE = "20";
  private static final String CFG_PARTITIONS_PER_TASK_VALUE = "15";
  private static final String CFG_PARTITIONS_FULLNESS_THRESHOLD_PCT_VALUE = "75";
  private static final String CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT_VALUE = "true";
  private static final String CFG_ZK_ADDRESS_VALUE = "dummyZk";
  private static final String CFG_ZK_SESSION_TIMEOUT_VALUE = "1000";
  private static final String CFG_ZK_CONNECTION_TIMEOUT_VALUE = "2000";
  private static final String CFG_CLUSTER_NAME_VALUE = "dummyCluster";
  private static final String INVALID_INTEGER_VALUE = "-1";

  @Test
  public void configValuesCorrectlyAssignedTest() {
    Properties props = new Properties();
    props.setProperty(CFG_MAX_TASKS, CFG_MAX_TASKS_VALUE);
    props.setProperty(CFG_IMBALANCE_THRESHOLD, CFG_IMBALANCE_THRESHOLD_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_MAX_PARTITION_PER_TASK, CFG_MAX_PARTITION_PER_TASK_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_PARTITIONS_PER_TASK, CFG_PARTITIONS_PER_TASK_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_PARTITION_FULLNESS_THRESHOLD_PCT,
        CFG_PARTITIONS_FULLNESS_THRESHOLD_PCT_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT,
        CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_ZK_ADDRESS, CFG_ZK_ADDRESS_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_ZK_SESSION_TIMEOUT, CFG_ZK_SESSION_TIMEOUT_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_ZK_CONNECTION_TIMEOUT, CFG_ZK_CONNECTION_TIMEOUT_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_CLUSTER_NAME, CFG_CLUSTER_NAME_VALUE);

    PartitionAssignmentStrategyConfig config = new PartitionAssignmentStrategyConfig(props);
    Assert.assertEquals(config.getMaxTasks(), Optional.of(Integer.parseInt(CFG_MAX_TASKS_VALUE)));
    Assert.assertEquals(config.getImbalanceThreshold(), Integer.parseInt(CFG_IMBALANCE_THRESHOLD_VALUE));
    Assert.assertEquals(config.getPartitionsPerTask(), Integer.parseInt(CFG_PARTITIONS_PER_TASK_VALUE));
    Assert.assertEquals(config.getMaxPartitions(), Integer.parseInt(CFG_MAX_PARTITION_PER_TASK_VALUE));
    Assert.assertEquals(config.getPartitionFullnessThresholdPct(),
        Integer.parseInt(CFG_PARTITIONS_FULLNESS_THRESHOLD_PCT_VALUE));
    Assert.assertEquals(config.isElasticTaskAssignmentEnabled(), Boolean.parseBoolean(CFG_ENABLE_ELASTIC_TASK_ASSIGNMENT_VALUE));
    Assert.assertEquals(config.getZkAddress(), CFG_ZK_ADDRESS_VALUE);
    Assert.assertEquals(config.getZkSessionTimeout(), Integer.parseInt(CFG_ZK_SESSION_TIMEOUT_VALUE));
    Assert.assertEquals(config.getZkConnectionTimeout(), Integer.parseInt(CFG_ZK_CONNECTION_TIMEOUT_VALUE));
    Assert.assertEquals(config.getCluster(), CFG_CLUSTER_NAME_VALUE);
  }

  @Test
  public void configValuesRevertedToEmptyWhenInvalidTest() {
    Properties props = new Properties();
    props.setProperty(CFG_MAX_TASKS, INVALID_INTEGER_VALUE);
    props.setProperty(CFG_IMBALANCE_THRESHOLD, INVALID_INTEGER_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_MAX_PARTITION_PER_TASK, INVALID_INTEGER_VALUE);
    props.setProperty(PartitionAssignmentStrategyConfig.CFG_PARTITIONS_PER_TASK, INVALID_INTEGER_VALUE);

    PartitionAssignmentStrategyConfig config = new PartitionAssignmentStrategyConfig(props);
    Assert.assertEquals(config.getMaxTasks(), Optional.empty());
    Assert.assertEquals(config.getImbalanceThreshold(), DEFAULT_IMBALANCE_THRESHOLD);
    Assert.assertEquals(config.getMaxPartitions(), Integer.MAX_VALUE);
    Assert.assertEquals(config.getPartitionsPerTask(), DEFAULT_PARTITIONS_PER_TASK);
  }

  @Test
  public void configValuesSetToDefaultWhenNotProvidedTest() {
    PartitionAssignmentStrategyConfig config = new PartitionAssignmentStrategyConfig(new Properties());
    Assert.assertEquals(config.getMaxTasks(), Optional.empty());
    Assert.assertEquals(config.getImbalanceThreshold(), DEFAULT_IMBALANCE_THRESHOLD);
    Assert.assertEquals(config.getMaxPartitions(), Integer.MAX_VALUE);
    Assert.assertEquals(config.getPartitionsPerTask(), DEFAULT_PARTITIONS_PER_TASK);
  }
}
