package com.linkedin.datastream.server.api.strategy;

import java.util.Properties;


/**
 * Factory to create the assignment strategy
 */
public interface AssignmentStrategyFactory {

  AssignmentStrategy createStrategy(Properties assignmentStrategyProperties);
}
