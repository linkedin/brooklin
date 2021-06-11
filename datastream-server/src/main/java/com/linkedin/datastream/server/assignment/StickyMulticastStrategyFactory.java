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


/**
 * A factory for creating {@link StickyMulticastStrategy} instances
 */
public class StickyMulticastStrategyFactory implements AssignmentStrategyFactory {
  public static final String CFG_IMBALANCE_THRESHOLD = "imbalanceThreshold";
  public static final Integer DEFAULT_IMBALANCE_THRESHOLD = 1;

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    VerifiableProperties props = new VerifiableProperties(assignmentStrategyProperties);
    int cfgMaxTasks = props.getInt(CFG_MAX_TASKS, Integer.MIN_VALUE);
    Optional<Integer> maxTasks = cfgMaxTasks > 0 ? Optional.of(cfgMaxTasks) : Optional.empty();
    int cfgImbalanceThreshold = props.getInt(CFG_IMBALANCE_THRESHOLD, DEFAULT_IMBALANCE_THRESHOLD);
    return new StickyMulticastStrategy(maxTasks, cfgImbalanceThreshold);
  }
}
