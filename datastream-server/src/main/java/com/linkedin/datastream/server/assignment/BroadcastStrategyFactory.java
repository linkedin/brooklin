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


/**
 * A factory for creating {@link BroadcastStrategy} instances
 */
public class BroadcastStrategyFactory implements AssignmentStrategyFactory {
  // the number of datastream tasks to create for a datastream
  public static final String CFG_MAX_TASKS = "maxTasks";
  public static final String CFG_MAX_TASKS_PER_INSTANCE = "maxTasksPerInstance";

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    VerifiableProperties props = new VerifiableProperties(assignmentStrategyProperties);
    int cfgMaxTasks = props.getInt(CFG_MAX_TASKS, Integer.MIN_VALUE);
    Optional<Integer> maxTasks = cfgMaxTasks > 0 ? Optional.of(cfgMaxTasks) : Optional.empty();

    int cfgMaxTasksPerInstance = props.getInt(CFG_MAX_TASKS_PER_INSTANCE, Integer.MIN_VALUE);
    Optional<Integer> maxTasksPerInstance = cfgMaxTasksPerInstance > 0 ? Optional.of(cfgMaxTasksPerInstance) : Optional.empty();

    return maxTasksPerInstance.isPresent() ? new BroadcastStrategy(maxTasks, maxTasksPerInstance) : new BroadcastStrategy(maxTasks);
  }
}
