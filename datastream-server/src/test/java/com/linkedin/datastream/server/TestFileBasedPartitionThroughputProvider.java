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
