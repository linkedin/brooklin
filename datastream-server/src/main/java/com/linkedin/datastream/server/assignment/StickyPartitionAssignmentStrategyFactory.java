/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Optional;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;


/**
 * A factory for creating {@link StickyPartitionAssignmentStrategy} instances
 */
public class StickyPartitionAssignmentStrategyFactory implements AssignmentStrategyFactory {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategyFactory.class.getName());

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    PartitionAssignmentStrategyConfig config = new PartitionAssignmentStrategyConfig(assignmentStrategyProperties);

    boolean enableElasticTaskAssignment = config.isElasticTaskAssignmentEnabled();
    // Create the zookeeper client
    Optional<ZkClient> zkClient = Optional.empty();
    try {
      zkClient = constructZooKeeperClient(enableElasticTaskAssignment, config.getZkAddress(),
          config.getZkSessionTimeout(), config.getZkSessionTimeout());
    } catch (IllegalStateException ex) {
      LOG.warn("Disabling elastic task assignment as zkClient initialization failed");
      enableElasticTaskAssignment = false;
    }

    return new StickyPartitionAssignmentStrategy(config.getMaxTasks(), config.getImbalanceThreshold(),
        config.getMaxPartitions(), enableElasticTaskAssignment, config.getPartitionsPerTask(),
        config.getPartitionFullnessThresholdPct(), zkClient, config.getCluster());
  }

  protected Optional<ZkClient> constructZooKeeperClient(boolean enableElasticTaskAssignment, String zkAddress,
      int zkSessionTimeout, int zkConnectionTimeout) {
    Optional<ZkClient> zkClient = Optional.empty();
    if (enableElasticTaskAssignment && StringUtils.isBlank(zkAddress)) {
      LOG.warn("ZkAddress is not present or empty");
      throw new IllegalStateException("ZkAddress is empty or not provided");
    }

    if (enableElasticTaskAssignment) {
      zkClient = Optional.of(new ZkClient(zkAddress, zkSessionTimeout, zkConnectionTimeout));
    }

    return zkClient;
  }
}
