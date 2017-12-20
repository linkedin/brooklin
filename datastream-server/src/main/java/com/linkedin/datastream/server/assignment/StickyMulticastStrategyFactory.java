package com.linkedin.datastream.server.assignment;

import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;
import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.DEFAULT_MAX_TASKS;


public class StickyMulticastStrategyFactory implements AssignmentStrategyFactory {

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    VerifiableProperties props = new VerifiableProperties(assignmentStrategyProperties);
    int maxTasks = props.getInt(CFG_MAX_TASKS, DEFAULT_MAX_TASKS);
    return new StickyMulticastStrategy(maxTasks);
  }
}
