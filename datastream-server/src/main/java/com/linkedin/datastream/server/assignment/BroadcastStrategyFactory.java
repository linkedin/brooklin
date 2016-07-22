package com.linkedin.datastream.server.assignment;

import java.util.Properties;

import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;


public class BroadcastStrategyFactory implements AssignmentStrategyFactory {

  @Override
  public AssignmentStrategy createStrategy(Properties assignmentStrategyProperties) {
    return new BroadcastStrategy();
  }
}
