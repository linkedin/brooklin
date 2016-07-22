package com.linkedin.datastream.server.assignment;

import java.util.Properties;

import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;


public class LoadbalancingStrategyFactory implements AssignmentStrategyFactory {

  @Override
  public AssignmentStrategy createStrategy(Properties properties) {
    return new LoadbalancingStrategy(properties);
  }
}
