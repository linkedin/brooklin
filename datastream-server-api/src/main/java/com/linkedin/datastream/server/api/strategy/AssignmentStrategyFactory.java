/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.strategy;

import java.util.Properties;


/**
 * An abstraction of an {@link AssignmentStrategy} factory
 */
public interface AssignmentStrategyFactory {

  /**
   * Create a {@link AssignmentStrategy} instance
   * @param assignmentStrategyProperties Strategy configuration properties
   */
  AssignmentStrategy createStrategy(Properties assignmentStrategyProperties);
}
