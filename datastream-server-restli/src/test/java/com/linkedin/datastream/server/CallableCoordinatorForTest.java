/**
 *  Copyright 2023 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Properties;

/**
 * Callable Coordinator is used for overriding coordinator behaviors for tests
 */
public interface CallableCoordinatorForTest {
  /**
   * invoking constructor of coordinator with params,
   * - datastreamCache to maintain all the datastreams in the cluster.
   * - properties to use while creating coordinator.
   * */
  Coordinator invoke(CachedDatastreamReader cachedDatastreamReader, Properties properties);
}