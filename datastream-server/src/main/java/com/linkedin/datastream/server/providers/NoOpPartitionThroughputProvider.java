/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.util.HashMap;

import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroup;


/**
 * Dummy implementation of {@link PartitionThroughputProvider}
 */
public class NoOpPartitionThroughputProvider implements PartitionThroughputProvider {
  @Override
  public ClusterThroughputInfo getThroughputInfo(String clusterName) {
    return null;
  }

  @Override
  public ClusterThroughputInfo getThroughputInfo(DatastreamGroup datastreamGroup) {
    return null;
  }

  @Override
  public HashMap<String, ClusterThroughputInfo> getThroughputInfo() {
    return null;
  }
}
