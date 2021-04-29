package com.linkedin.datastream.server;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestFileBasedPartitionThroughputProvider {

  @Test
  public void getPartitionThroughputForMetricsTest() {
    FileBasedPartitionThroughputProvider provider = new FileBasedPartitionThroughputProvider();
    ClusterThroughputInfo stats = provider.getThroughputInfo("metrics");
    Assert.assertNotNull(stats);
  }
}
