/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.strategy;

import java.util.Properties;


/**
 * Factory to create the assignment strategy
 */
public interface AssignmentStrategyFactory {

  AssignmentStrategy createStrategy(Properties assignmentStrategyProperties);
}
