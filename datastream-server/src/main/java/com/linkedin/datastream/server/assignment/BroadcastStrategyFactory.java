package com.linkedin.datastream.server.assignment;

import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;


public class BroadcastStrategyFactory implements AssignmentStrategyFactory {
  public static final String CFG_MAX_TASKS = "maxTasks";
  public static final int DEFAULT_MAX_TASKS = 12;

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    VerifiableProperties props = new VerifiableProperties(assignmentStrategyProperties);
    int maxTasks = props.getInt(CFG_MAX_TASKS, DEFAULT_MAX_TASKS);
    return new BroadcastStrategy(maxTasks);
  }
}
