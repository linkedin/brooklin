/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

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
  protected PartitionAssignmentStrategyConfig _config;

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    _config = new PartitionAssignmentStrategyConfig(assignmentStrategyProperties);

    boolean enableElasticTaskAssignment = _config.isElasticTaskAssignmentEnabled();
    // Create the zookeeper client
    ZkClient zkClient = null;
    try {
      zkClient = constructZooKeeperClient();
    } catch (IllegalStateException ex) {
      LOG.warn("Disabling elastic task assignment as zkClient initialization failed", ex);
      enableElasticTaskAssignment = false;
    }

    return new StickyPartitionAssignmentStrategy(_config.getMaxTasks(), _config.getImbalanceThreshold(),
        _config.getMaxPartitions(), enableElasticTaskAssignment, _config.getPartitionsPerTask(),
        _config.getPartitionFullnessThresholdPct(), zkClient, _config.getCluster());
  }

  protected ZkClient constructZooKeeperClient() {
    if (!_config.isElasticTaskAssignmentEnabled()) {
      return null;
    }

    if (StringUtils.isBlank(_config.getZkAddress())) {
      LOG.warn("ZkAddress is not present or empty");
      throw new IllegalStateException("ZkAddress is empty or not provided");
    }

    return new ZkClient(_config.getZkAddress(), _config.getZkSessionTimeout(), _config.getZkConnectionTimeout());
  }
}
