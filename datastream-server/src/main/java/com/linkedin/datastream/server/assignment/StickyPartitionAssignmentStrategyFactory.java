/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Optional;
import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;
import static com.linkedin.datastream.server.assignment.StickyMulticastStrategyFactory.CFG_IMBALANCE_THRESHOLD;


/**
 * A factory for creating {@link StickyPartitionAssignmentStrategy} instances
 */
public class StickyPartitionAssignmentStrategyFactory implements AssignmentStrategyFactory {
  public static final String CFG_MAX_PARTITION_PER_TASK = "maxPartitionsPerTask";


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
    return new StickyPartitionAssignmentStrategy(maxTasks, imbalanceThreshold, maxPartitions);
  }
}
