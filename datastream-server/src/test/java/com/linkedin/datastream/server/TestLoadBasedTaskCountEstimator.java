/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.linkedin.datastream.server.assignment.LoadBasedTaskCountEstimator;
import com.linkedin.datastream.server.providers.FileBasedPartitionThroughputProvider;


/**
 * Tests for {@link LoadBasedTaskCountEstimator}
 */
public class TestLoadBasedTaskCountEstimator {
  private static final String THROUGHPUT_FILE_NAME = "partitionThroughput.json";
  FileBasedPartitionThroughputProvider _provider;

  /**
   * Test class initialization code
   */
  @BeforeClass
  public void setup() {
    _provider = new FileBasedPartitionThroughputProvider(THROUGHPUT_FILE_NAME);
  }

  @Test
  public void emptyAssignmentReturnsZeroTasksTest() {
    ClusterThroughputInfo throughputInfo = _provider.getThroughputInfo("pizza");
    List<String> assignedPartitions = Collections.emptyList();
    List<String> unassignedPartitions = Collections.emptyList();
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator();
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);
    Assert.assertEquals(taskCount, 0);
  }

  @Test
  public void lowThroughputAssignmentReturnsOneTaskTest() {
    ClusterThroughputInfo throughputInfo = _provider.getThroughputInfo("pizza");
    List<String> assignedPartitions = new ArrayList<>();
    assignedPartitions.add("Pepperoni-1");
    List<String> unassignedPartitions = Collections.emptyList();
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator();
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);
    Assert.assertEquals(taskCount, 1);
  }

  @Test
  public void highThroughputAssignmentTest() {
    ClusterThroughputInfo throughputInfo = _provider.getThroughputInfo("ice-cream");
    List<String> assignedPartitions = Collections.emptyList();
    List<String> unassignedPartitions = new ArrayList<>(throughputInfo.getPartitionInfoMap().keySet());
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator();
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);

    int throughputSum = throughputInfo.getPartitionInfoMap().values().stream().mapToInt(
        PartitionThroughputInfo::getBytesInKBRate).sum();
    Assert.assertTrue(taskCount >=
        throughputSum / (LoadBasedTaskCountEstimator.TASK_CAPACITY_MBPS_DEFAULT * 1024));
  }

  @Test
  public void highThroughputAssignmentTest2() {
    ClusterThroughputInfo throughputInfo = _provider.getThroughputInfo("donut");
    List<String> assignedPartitions = Collections.emptyList();
    List<String> unassignedPartitions = new ArrayList<>(throughputInfo.getPartitionInfoMap().keySet());
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator();
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);

    int throughputSum = throughputInfo.getPartitionInfoMap().values().stream().mapToInt(
        PartitionThroughputInfo::getBytesInKBRate).sum();
    Assert.assertTrue(taskCount >=
        throughputSum / (LoadBasedTaskCountEstimator.TASK_CAPACITY_MBPS_DEFAULT * 1024));
    int totalNumPartitions = assignedPartitions.size() + unassignedPartitions.size();
    Assert.assertTrue(taskCount <= totalNumPartitions);
  }
}
