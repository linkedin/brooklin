/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

/**
 * An interface that resolves the source Kafka cluster from the given {@link DatastreamGroup} instance
 */
public interface DatastreamSourceClusterResolver {

  /**
   * Given a datastream group, gets the name of the source cluster
   * @param datastreamGroup Datastream group
   * @return The name of the source cluster
   */
  String getSourceCluster(DatastreamGroup datastreamGroup);
}
