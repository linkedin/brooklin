/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
