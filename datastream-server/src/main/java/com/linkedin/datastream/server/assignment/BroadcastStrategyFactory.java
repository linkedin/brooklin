package com.linkedin.datastream.server.assignment;

import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;


public class BroadcastStrategyFactory implements AssignmentStrategyFactory {
  // the number of datastream tasks to create for a datastream
  public static final String CFG_MAX_TASKS = "maxTasks";
  // the max number of datastream tasks that can be assigned to each instance, for a datastream
  public static final String CFG_LIMIT_MAX_TASKS = "dsTaskLimitPerInstance";
  public static final int DEFAULT_MAX_TASKS = 12;
  public static final int DEFAULT_DS_TASK_LIMIT_PER_INSTANCE = 1;


  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    VerifiableProperties props = new VerifiableProperties(assignmentStrategyProperties);
    int maxTasks = props.getInt(CFG_MAX_TASKS, DEFAULT_MAX_TASKS);
    int dsTaskLimitPerInstance = props.getInt(CFG_LIMIT_MAX_TASKS, DEFAULT_DS_TASK_LIMIT_PER_INSTANCE);
    return new BroadcastStrategy(maxTasks, dsTaskLimitPerInstance);
  }
}
