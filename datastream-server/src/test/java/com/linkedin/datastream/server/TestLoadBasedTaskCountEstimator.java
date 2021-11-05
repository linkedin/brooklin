/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
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
  private static final int TASK_CAPACITY_MBPS = 4;
  private static final int TASK_CAPACITY_UTILIZATION_PCT = 90;
  private static final int DEFAULT_BYTES_IN_KB_RATE = 5;
  private static final int DEFAULT_MSGS_IN_RATE = 5;

  private FileBasedPartitionThroughputProvider _provider;

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
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(TASK_CAPACITY_MBPS,
        TASK_CAPACITY_UTILIZATION_PCT, DEFAULT_BYTES_IN_KB_RATE, DEFAULT_MSGS_IN_RATE);
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);
    Assert.assertEquals(taskCount, 0);
  }

  @Test
  public void lowThroughputAssignmentReturnsOneTaskTest() {
    ClusterThroughputInfo throughputInfo = _provider.getThroughputInfo("pizza");
    List<String> assignedPartitions = new ArrayList<>();
    assignedPartitions.add("Pepperoni-1");
    List<String> unassignedPartitions = Collections.emptyList();
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(TASK_CAPACITY_MBPS,
        TASK_CAPACITY_UTILIZATION_PCT, DEFAULT_BYTES_IN_KB_RATE, DEFAULT_MSGS_IN_RATE);
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);
    Assert.assertEquals(taskCount, 1);
  }

  @Test
  public void highThroughputAssignmentTest() {
    ClusterThroughputInfo throughputInfo = _provider.getThroughputInfo("ice-cream");
    List<String> assignedPartitions = Collections.emptyList();
    List<String> unassignedPartitions = new ArrayList<>(throughputInfo.getPartitionInfoMap().keySet());
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(TASK_CAPACITY_MBPS,
        TASK_CAPACITY_UTILIZATION_PCT, DEFAULT_BYTES_IN_KB_RATE, DEFAULT_MSGS_IN_RATE);
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);

    int throughputSum = throughputInfo.getPartitionInfoMap().values().stream().mapToInt(
        PartitionThroughputInfo::getBytesInKBRate).sum();
    double taskCapacityCoefficient = TASK_CAPACITY_UTILIZATION_PCT / 100.0;
    Assert.assertTrue(taskCount >=
        throughputSum / (TASK_CAPACITY_MBPS * taskCapacityCoefficient * 1024));
  }

  @Test
  public void highThroughputAssignmentTest2() {
    ClusterThroughputInfo throughputInfo = _provider.getThroughputInfo("donut");
    List<String> assignedPartitions = Collections.emptyList();
    List<String> unassignedPartitions = new ArrayList<>(throughputInfo.getPartitionInfoMap().keySet());
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(TASK_CAPACITY_MBPS,
        TASK_CAPACITY_UTILIZATION_PCT, DEFAULT_BYTES_IN_KB_RATE, DEFAULT_MSGS_IN_RATE);
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);
    Assert.assertEquals(taskCount, unassignedPartitions.size());
  }

  @Test
  public void partitionsHaveDefaultWeightTest() {
    ClusterThroughputInfo throughputInfo = new ClusterThroughputInfo("dummy", new HashMap<>());
    List<String> assignedPartitions = Collections.emptyList();
    List<String> unassignedPartitions = Arrays.asList("P1", "P2");
    LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(TASK_CAPACITY_MBPS,
        TASK_CAPACITY_UTILIZATION_PCT, DEFAULT_BYTES_IN_KB_RATE, DEFAULT_MSGS_IN_RATE);
    int taskCount = estimator.getTaskCount(throughputInfo, assignedPartitions, unassignedPartitions);
    Assert.assertTrue(taskCount > 0);
  }
}
