/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.DatastreamSourceClusterResolver;
import com.linkedin.datastream.server.DummyDatastreamSourceClusterResolver;
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
    _config = new PartitionAssignmentStrategyConfig(assignmentStrategyProperties);

    boolean enableElasticTaskAssignment = _config.isElasticTaskAssignmentEnabled();
    // Create the zookeeper client
    Optional<ZkClient> zkClient = Optional.empty();
    try {
      zkClient = constructZooKeeperClient();
    } catch (IllegalStateException ex) {
      LOG.warn("Disabling elastic task assignment as zkClient initialization failed", ex);
      enableElasticTaskAssignment = false;
    }

    PartitionThroughputProvider provider = constructPartitionThroughputProvider();
    DatastreamSourceClusterResolver clusterResolver = constructDatastreamSourceClusterResolver();

    return new LoadBasedPartitionAssignmentStrategy(provider, clusterResolver, _config.getMaxTasks(),
        _config.getImbalanceThreshold(), _config.getMaxPartitions(), enableElasticTaskAssignment,
        _config.getPartitionsPerTask(), _config.getPartitionFullnessThresholdPct(), _config.getTaskCapacityMBps(),
        _config.getTaskCapacityUtilizationPct(), _config.getThroughputInfoFetchTimeoutMs(),
        _config.getThroughputInfoFetchRetryPeriodMs(), zkClient, _config.getCluster());
  }

  protected PartitionThroughputProvider constructPartitionThroughputProvider() {
    return new NoOpPartitionThroughputProvider();
  }

  protected DatastreamSourceClusterResolver constructDatastreamSourceClusterResolver() {
    return new DummyDatastreamSourceClusterResolver();
  }
}
