/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.providers.NoOpPartitionThroughputProvider;
import com.linkedin.datastream.server.providers.PartitionThroughputProvider;


/**
 * A factory for creating {@link LoadBasedPartitionAssignmentStrategy} instances
 */
public class LoadBasedPartitionAssignmentStrategyFactory extends StickyPartitionAssignmentStrategyFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedPartitionAssignmentStrategyFactory.class.getName());

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    _config = new LoadBasedPartitionAssignmentStrategyConfig(assignmentStrategyProperties);
    LoadBasedPartitionAssignmentStrategyConfig config = (LoadBasedPartitionAssignmentStrategyConfig) _config;
    boolean enableElasticTaskAssignment = _config.isElasticTaskAssignmentEnabled();
    // Create the zookeeper client
    ZkClient zkClient = null;
    try {
      zkClient = constructZooKeeperClient();
    } catch (IllegalStateException ex) {
      LOG.warn("Disabling elastic task assignment as zkClient initialization failed", ex);
      enableElasticTaskAssignment = false;
    }

    PartitionThroughputProvider provider = constructPartitionThroughputProvider();

    return new LoadBasedPartitionAssignmentStrategy(provider, _config.getMaxTasks(),
        _config.getImbalanceThreshold(), _config.getMaxPartitions(), enableElasticTaskAssignment,
        _config.getPartitionsPerTask(), _config.getPartitionFullnessThresholdPct(), config.getTaskCapacityMBps(),
        config.getTaskCapacityUtilizationPct(), config.getThroughputInfoFetchTimeoutMs(),
        config.getThroughputInfoFetchRetryPeriodMs(), zkClient, _config.getCluster());
  }

  protected PartitionThroughputProvider constructPartitionThroughputProvider() {
    return new NoOpPartitionThroughputProvider();
  }
}
