/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.server.providers.FileBasedPartitionThroughputProvider;


/**
 * Tests for {@link FileBasedPartitionThroughputProvider}
 */
public class TestFileBasedPartitionThroughputProvider {

  @Test
  public void getPartitionThroughputForMetricsTest() {
    FileBasedPartitionThroughputProvider provider = new FileBasedPartitionThroughputProvider();
    ClusterThroughputInfo stats = provider.getThroughputInfo("cookie");
    Assert.assertNotNull(stats);
  }

  @Test
  public void getPartitionThroughputForAllClustersTest() {
    FileBasedPartitionThroughputProvider provider = new FileBasedPartitionThroughputProvider();
    HashMap<String, ClusterThroughputInfo> clusterInfoMap = provider.getThroughputInfo();
    Assert.assertNotNull(clusterInfoMap);
  }
}
