/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.server.providers.FileBasedPartitionThroughputProvider;
import com.linkedin.datastream.server.providers.PartitionThroughputProvider;


/**
 * Tests for {@link FileBasedPartitionThroughputProvider}
 */
public class TestFileBasedPartitionThroughputProvider {

  private static final String THROUGHPUT_FILE_NAME = "partitionThroughput.json";

  @Test
  public void getPartitionThroughputForMetricsTest() {
    String clusterName = "cookie";
    FileBasedPartitionThroughputProvider provider = new FileBasedPartitionThroughputProvider(THROUGHPUT_FILE_NAME);
    ClusterThroughputInfo stats = provider.getThroughputInfo(clusterName);
    Assert.assertNotNull(stats);
    Assert.assertEquals(stats.getClusterName(), clusterName);
  }

  @Test
  public void getPartitionThroughputForAllClustersTest() {
    FileBasedPartitionThroughputProvider provider = new FileBasedPartitionThroughputProvider(THROUGHPUT_FILE_NAME);
    Map<String, ClusterThroughputInfo> clusterInfoMap = provider.getThroughputInfo();
    Assert.assertNotNull(clusterInfoMap);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void getPartitionThroughputThrowsWhenFileNotFound() {
    String dummyFileName = "dummy.json";
    PartitionThroughputProvider provider = new FileBasedPartitionThroughputProvider(dummyFileName);
    Map<String, ClusterThroughputInfo> clusterInfoMap = provider.getThroughputInfo();
  }
}
