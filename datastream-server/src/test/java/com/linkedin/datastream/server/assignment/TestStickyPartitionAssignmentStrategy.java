/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.jetbrains.annotations.NotNull;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.datastream.testutil.MetricsTestUtils;

import static com.linkedin.datastream.server.assignment.StickyMulticastStrategyFactory.DEFAULT_IMBALANCE_THRESHOLD;
import static com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategy.CLASS_NAME;
import static com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategy.ELASTIC_TASK_PARAMETERS_NEED_ADJUSTMENT;
import static com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategy.NUM_TASKS;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link StickyPartitionAssignmentStrategy}
 */
public class TestStickyPartitionAssignmentStrategy {
  private EmbeddedZookeeper _embeddedZookeeper;
  private String _clusterName;
  private ZkClient _zkClient;
  private DynamicMetricsManager _metricsManager;

  /**
   * Test class initialization code
   */
  @BeforeClass
  public void setupClass() throws IOException {
    _metricsManager = DynamicMetricsManager.createInstance(new MetricRegistry(), "TestStickyPartitionAssignment");
  }

  @BeforeMethod
  public void setup() throws IOException {
    _clusterName = "testcluster";
    _embeddedZookeeper = new EmbeddedZookeeper();
    String zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
    _zkClient = new ZkClient(zkConnectionString);
  }

  @AfterMethod
  public void teardown() throws Exception {
    _embeddedZookeeper.shutdown();
  }

  /**
   * Test class teardown code
   */
  @AfterClass
  public void teardownClass() throws Exception {
    // A hack to force clean up DynamicMetricsManager
    Field field = DynamicMetricsManager.class.getDeclaredField("_instance");
    try {
      field.setAccessible(true);
      field.set(null, null);
    } finally {
      field.setAccessible(false);
    }
  }

  @Test
  public void testCreateAssignmentAcrossAllTasks() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);

      Set<DatastreamTask> taskSet = new HashSet<>();
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 1, 3);
      } else {
        datastreams = generateDatastreams("ds", 1);
      }

      Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 1, 3, true);
      assignment.put("instance1", taskSet);

      List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

      assignment = strategy.assignPartitions(assignment, partitionsMetadata);

      for (DatastreamTask task : assignment.get("instance1")) {
        Assert.assertEquals(task.getPartitionsV2().size(), 1);
      }
    });
  }

  @Test
  public void testAddPartitions() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);
      Set<DatastreamTask> taskSet = new HashSet<>();
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 1, 3);
      } else {
        datastreams = generateDatastreams("ds", 1);
      }

      Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 1, 3, true);
      assignment.put("instance1", taskSet);

      List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

      assignment = strategy.assignPartitions(assignment, partitionsMetadata);

      List<String> newPartitions = ImmutableList.of("t-0", "t-1", "t1-0", "t2-0", "t2-1", "t2-2");
      DatastreamGroupPartitionsMetadata newPartitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), newPartitions);

      Map<String, Set<DatastreamTask>> newAssignment = strategy.assignPartitions(assignment, newPartitionsMetadata);

      for (DatastreamTask task : newAssignment.get("instance0")) {
        Assert.assertEquals(task.getPartitionsV2().size(), 2);
      }

      Map<String, List<DatastreamTask>> taskToCleanup = strategy.getTasksToCleanUp(datastreams, newAssignment);
      Assert.assertEquals(taskToCleanup.size(), 0);

      // Adding the dependency task as well in the assignment list to simulate the scenario where
      // the dependency task nodes are not deleted and the leader gets interrupted, OOM or hit session expiry.
      // The next leader should be able to identify and cleanup.
      Map<String, Set<DatastreamTask>> finalAssignment = assignment;
      newAssignment.forEach((instance, taskSet1) -> taskSet1.addAll(finalAssignment.get(instance)));

      taskToCleanup = strategy.getTasksToCleanUp(datastreams, newAssignment);
      Assert.assertEquals(taskToCleanup.size(), 1);
      taskToCleanup.forEach((instance, taskList1) -> Assert.assertEquals(taskList1.size(), 3));
      Assert.assertEquals(new HashSet<>(taskToCleanup.get("instance0")), new HashSet<>(assignment.get("instance0")));
    });
  }

  @Test
  public void testCreateAssignmentFailureDueToUnlockedTask() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);
      Set<DatastreamTask> taskSet = new HashSet<>();
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 1, 3);
      } else {
        datastreams = generateDatastreams("ds", 1);
      }

      Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 1, 3, false);
      assignment.put("instance1", taskSet);

      List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");

      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

      Map<String, Set<DatastreamTask>> assignment2 = strategy.assignPartitions(assignment, partitionsMetadata);
      assignment2.put("instance2", taskSet);
      partitions = ImmutableList.of("t-0", "t-1", "t1-0", "t2-0");
      DatastreamGroupPartitionsMetadata partitionsMetadata2 =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);
      Assert.assertThrows(DatastreamRuntimeException.class,
          () -> strategy.assignPartitions(assignment2, partitionsMetadata2));
    });
  }

  @Test
  public void testAssignPartitionToInstanceWithoutTask() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 2, 1);
      } else {
        datastreams = generateDatastreams("ds", 2);
      }

      List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4");
      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);
      // Generate partition assignment
      Assert.assertThrows(IllegalArgumentException.class,
          () -> strategy.assignPartitions(new HashMap<>(), partitionsMetadata));
    });
  }

  @Test
  public void testAssignPartitionToInstanceWithoutTaskForDatastreamGroup() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 2, 2);
      } else {
        datastreams = generateDatastreams("ds", 2);
      }

      Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 3, 2, true);
      List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4");
      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(1), partitions);
      // Generate partition assignment
      Assert.assertThrows(IllegalArgumentException.class,
          () -> strategy.assignPartitions(assignment, partitionsMetadata));
    });
  }

  @Test
  public void testMovePartition() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 2, 2);
      } else {
        datastreams = generateDatastreams("ds", 2);
      }
      Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 3, 2, true);
      List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4", "t-5", "t-6", "t-7");
      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);
      // Generate partition assignment
      assignment = strategy.assignPartitions(assignment, partitionsMetadata);

      Map<String, Set<String>> targetAssignment = new HashMap<>();
      targetAssignment.put("instance2", ImmutableSet.of("t-3", "t-2", "t-1", "t-5"));
      targetAssignment.put("instance1", ImmutableSet.of("t-0", "t-10"));

      assignment = strategy.movePartitions(assignment, targetAssignment, partitionsMetadata);

      Assert.assertTrue(getPartitionsFromTask(assignment.get("instance2")).contains("t-1"));
      Assert.assertTrue(getPartitionsFromTask(assignment.get("instance2")).contains("t-2"));
      Assert.assertTrue(getPartitionsFromTask(assignment.get("instance2")).contains("t-3"));
      Assert.assertTrue(getPartitionsFromTask(assignment.get("instance2")).contains("t-5"));

      Assert.assertTrue(getPartitionsFromTask(assignment.get("instance1")).contains("t-0"));
      Assert.assertFalse(getPartitionsFromTask(assignment.get("instance1")).contains("t-10"));

      Assert.assertEquals(getTotalPartitions(assignment), 8);
    });
  }

  @Test
  public void testMovePartitionToInstanceWithoutTask() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 2, 2);
      } else {
        datastreams = generateDatastreams("ds", 2);
      }

      Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 3, 2, true);
      List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4");
      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);
      // Generate partition assignment
      assignment = strategy.assignPartitions(assignment, partitionsMetadata);
      assignment.put("empty", Collections.emptySet());

      Map<String, Set<String>> targetAssignment = new HashMap<>();
      targetAssignment.put("empty", ImmutableSet.of("t-3", "t-2", "t-1", "t-5"));
      Map<String, Set<DatastreamTask>> finalAssignment = assignment;
      Assert.assertThrows(DatastreamRuntimeException.class,
          () -> strategy.movePartitions(finalAssignment, targetAssignment, partitionsMetadata));
    });
  }

  @Test
  public void testRemovePartitions() {
    Set<Boolean> elasticTaskAssignmentEnabledSet = new HashSet<>(Arrays.asList(true, false));

    elasticTaskAssignmentEnabledSet.forEach(elasticAssignmentEnabled -> {
      StickyPartitionAssignmentStrategy strategy =
          createStickyPartitionAssignmentStrategy(3, 90, elasticAssignmentEnabled,
              getZkClient(elasticAssignmentEnabled), _clusterName);
      List<DatastreamGroup> datastreams;
      if (elasticAssignmentEnabled) {
        datastreams = generateDatastreams("ds", 1, 2);
      } else {
        datastreams = generateDatastreams("ds", 1);
      }

      Map<String, Set<DatastreamTask>> assignment = generateEmptyAssignment(datastreams, 3, 2, true);

      List<String> partitions = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4", "t-5", "t-6");
      DatastreamGroupPartitionsMetadata partitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

      // Generate partition assignment
      assignment = strategy.assignPartitions(assignment, partitionsMetadata);

      List<String> newPartitions = ImmutableList.of("t-1", "t-3", "t-4", "t-6");
      DatastreamGroupPartitionsMetadata newPartitionsMetadata =
          new DatastreamGroupPartitionsMetadata(datastreams.get(0), newPartitions);

      assignment = strategy.assignPartitions(assignment, newPartitionsMetadata);

      List<String> remainingPartitions = new ArrayList<>();
      for (String instance : assignment.keySet()) {
        for (DatastreamTask task : assignment.get(instance)) {
          remainingPartitions.addAll(task.getPartitionsV2());
        }
      }

      Assert.assertEquals(new HashSet<>(remainingPartitions), new HashSet<>(newPartitions));
    });
  }

  @Test
  public void testElasticTaskPartitionAssignmentFailsOnTooFewTasks() {
    int minTasks = 2;
    int partitionsPerTask = 2;
    int fullnessFactorPct = 50;
    // Create a strategy with partitionsPerTask = 2 and fullnessFactorPct as 50%
    StickyPartitionAssignmentStrategy strategy =
        createStickyPartitionAssignmentStrategy(partitionsPerTask, fullnessFactorPct, true, _zkClient, _clusterName);

    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1, minTasks);
    datastreams.forEach(dg -> _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, dg.getTaskPrefix())));

    Map<String, Set<DatastreamTask>> assignment = Collections.emptyMap();
    List<String> instances = new ArrayList<>();
    instances.add("instance1");

    // Assign tasks for 1 datastream to 1 instance. Validate minTasks number of tasks are created.
    assignment = strategy.assign(datastreams, instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), minTasks);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0");
    DatastreamGroupPartitionsMetadata partitionsMetadata =
        new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    // Partition assignment should fail because insufficient tasks are present to fit the partitions such that the
    // tasks are 50% full.
    Map<String, Set<DatastreamTask>> finalAssignment = assignment;
    Assert.assertThrows(DatastreamRuntimeException.class,
        () -> strategy.assignPartitions(finalAssignment, partitionsMetadata));
  }

  @Test
  public void testElasticTaskPartitionAssignmentCorrectlyAdjustsNumTasks() {
    int minTasks = 3;
    int partitionsPerTask = 4;
    int fullnessFactorPct = 50;
    // Create a strategy with partitionsPerTask = 4 and fullnessFactorPct as 50%
    StickyPartitionAssignmentStrategy strategy = createStickyPartitionAssignmentStrategy(partitionsPerTask,
        fullnessFactorPct, true, _zkClient, _clusterName);

    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1, minTasks);
    datastreams.forEach(dg -> _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, dg.getTaskPrefix())));

    Map<String, Set<DatastreamTask>> assignment = Collections.emptyMap();
    List<String> instances = new ArrayList<>();
    instances.add("instance1");

    // Assign tasks for 1 datastream to 1 instance. Validate minTasks number of tasks are created
    assignment = strategy.assign(datastreams, instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), minTasks);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0", "t2-0", "t1-1", "t1-2", "t0-3");
    DatastreamGroupPartitionsMetadata partitionsMetadata =
        new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    // Partition assignment should fail because insufficient tasks are present to fit the partitions such that the
    // tasks are 50% full. This should set the correct number of tasks needed so that the next assign() call creates
    // the correct number of tasks.
    Map<String, Set<DatastreamTask>> finalAssignment = assignment;
    Assert.assertThrows(DatastreamRuntimeException.class,
        () -> strategy.assignPartitions(finalAssignment, partitionsMetadata));

    // The assign call should create the expected number of tasks as set by partition assignment
    assignment = strategy.assign(datastreams, instances, assignment);
    int maxPartitionsPerTask = partitionsPerTask * fullnessFactorPct / 100;
    int numTasksNeeded = (partitions.size() / maxPartitionsPerTask)
        + (partitions.size() % maxPartitionsPerTask == 0 ? 0 : 1);
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);
    setupTaskLockForAssignment(assignment);

    // Partition assignment should go through
    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    validatePartitionAssignment(assignment, partitions, maxPartitionsPerTask, numTasksNeeded);

    MetricsTestUtils.verifyMetrics(strategy, DynamicMetricsManager.getInstance());
  }

  @Test
  public void testElasticTaskPartitionAssignmentRepeatedPartitionAssignments() {
    int minTasks = 3;
    int partitionsPerTask = 4;
    int fullnessFactorPct = 50;
    // Create a strategy with partitionsPerTask = 4 and fullnessFactorPct as 50%
    StickyPartitionAssignmentStrategy strategy = createStickyPartitionAssignmentStrategy(partitionsPerTask,
        fullnessFactorPct, true, _zkClient, _clusterName);

    List<DatastreamGroup> datastreams = generateDatastreams("testElasticTaskPartitionAssignmentRepeatedPartitionAssignments", 1, minTasks);
    datastreams.forEach(dg -> _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, dg.getTaskPrefix())));

    Map<String, Set<DatastreamTask>> assignment = Collections.emptyMap();
    List<String> instances = new ArrayList<>();
    instances.add("instance1");

    // Assign tasks for 1 datastream to 1 instance. Validate minTasks number of tasks are created
    assignment = strategy.assign(datastreams, instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), minTasks);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0", "t2-0", "t1-1", "t1-2", "t0-3");
    DatastreamGroupPartitionsMetadata partitionsMetadata =
        new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    // Partition assignment should fail because insufficient tasks are present to fit the partitions such that the
    // tasks are 50% full. This should set the correct number of tasks needed so that the next assign() call creates
    // the correct number of tasks.
    Map<String, Set<DatastreamTask>> finalAssignment = assignment;
    DatastreamGroupPartitionsMetadata finalPartitionsMetadata = partitionsMetadata;
    Assert.assertThrows(DatastreamRuntimeException.class,
        () -> strategy.assignPartitions(finalAssignment, finalPartitionsMetadata));

    // The assign call should create the expected number of tasks as set by partition assignment
    assignment = strategy.assign(datastreams, instances, assignment);
    int maxPartitionsPerTask = partitionsPerTask * fullnessFactorPct / 100;
    int numTasksNeeded = (partitions.size() / maxPartitionsPerTask)
        + (partitions.size() % maxPartitionsPerTask == 0 ? 0 : 1);
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);
    setupTaskLockForAssignment(assignment);

    // Partition assignment should go through
    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    validatePartitionAssignment(assignment, partitions, maxPartitionsPerTask, numTasksNeeded);

    // Decrease the number of partitions such that the partitions per task is now smaller than the fullness factor.
    // The number of tasks should remain the same.
    partitions = ImmutableList.of("t-0", "t-1", "t1-0");
    partitionsMetadata = new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);
    validatePartitionAssignment(assignment, partitions, maxPartitionsPerTask, numTasksNeeded);

    // Increase the number of partitions such that the partitions per task is now larger than the configured partitions
    // per task. The number of tasks should remain the same.
    partitions = ImmutableList.of("t-0", "t-1", "t1-0", "t2-0", "t1-1", "t1-2", "t0-3", "t42-0", "t2-1", "t21-0",
        "t22-0", "t21-1", "t21-2", "t20-3", "t3-0", "t3-1", "t31-0", "t32-0", "t31-1", "t31-2");
    partitionsMetadata = new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);
    maxPartitionsPerTask = partitions.size() / numTasksNeeded + (partitions.size() % numTasksNeeded == 0 ? 0 : 1);
    validatePartitionAssignment(assignment, partitions, maxPartitionsPerTask, numTasksNeeded);

    // Pass an empty assignment to simulate a datastream restart which wipes out the tasks. The number of tasks needed
    // should be reassessed on the next assignment when we add the datastream back.
    assignment = Collections.emptyMap();
    // Delete the ZK node for numTasks for this datastream to simulate deletion of this node on datastream stop.
    _zkClient.delete(KeyBuilder.datastreamNumTasks(_clusterName, datastreams.get(0).getTaskPrefix()));
    // Pass an empty datastream list, since on datastream stop, we expect the stopped datastream to be filtered out
    // from the list of valid datastreams passed to the assign() call.
    assignment = strategy.assign(Collections.emptyList(), instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), 0);

    // Assign tasks for 1 datastream to 1 instance. Validate minTasks number of tasks are created since the previous
    // assignment should have wiped out the previously calculated numTasks.
    assignment = strategy.assign(datastreams, instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), minTasks);

    // Partition assignment should fail because insufficient tasks are present to fit the partitions such that the
    // tasks are 50% full. This should set the correct number of tasks needed so that the next assign() call creates
    // the correct number of tasks.
    Map<String, Set<DatastreamTask>> finalAssignment1 = assignment;
    DatastreamGroupPartitionsMetadata finalPartitionsMetadata1 = partitionsMetadata;
    Assert.assertThrows(DatastreamRuntimeException.class,
        () -> strategy.assignPartitions(finalAssignment1, finalPartitionsMetadata1));

    // The assign call should create the expected number of tasks as set by partition assignment, and this should not
    // match the numTasksNeeded from the previous set of assignments.
    assignment = strategy.assign(datastreams, instances, assignment);
    maxPartitionsPerTask = partitionsPerTask * fullnessFactorPct / 100;
    int originalNumTasksNeeded = numTasksNeeded;
    numTasksNeeded = (partitions.size() / maxPartitionsPerTask)
        + (partitions.size() % maxPartitionsPerTask == 0 ? 0 : 1);
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);
    Assert.assertNotEquals(originalNumTasksNeeded, numTasksNeeded);
    setupTaskLockForAssignment(assignment);

    // Partition assignment should go through
    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);
    validatePartitionAssignment(assignment, partitions, maxPartitionsPerTask, numTasksNeeded);

    MetricsTestUtils.verifyMetrics(strategy, DynamicMetricsManager.getInstance());
    Gauge<?> gauge = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, datastreams.get(0).getName(), NUM_TASKS));
    Assert.assertEquals(gauge.getValue(), numTasksNeeded);
    gauge = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, datastreams.get(0).getName(), ELASTIC_TASK_PARAMETERS_NEED_ADJUSTMENT));
    Assert.assertEquals(gauge.getValue(), 0.0);
  }

  @Test
  public void testElasticTaskPartitionAssignmentCreatesMinTasksEvenForSmallPartitionCount() {
    int minTasks = 3;
    int partitionsPerTask = 2;
    int fullnessFactorPct = 50;
    // Create a strategy with partitionsPerTask = 2 and fullnessFactorPct as 50%
    StickyPartitionAssignmentStrategy strategy = createStickyPartitionAssignmentStrategy(partitionsPerTask,
        fullnessFactorPct, true, _zkClient, _clusterName);

    List<DatastreamGroup> datastreams = generateDatastreams("testElasticTaskPartitionAssignmentCreatesMinTasksEvenForSmallPartitionCount", 1, minTasks);
    datastreams.forEach(dg -> _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, dg.getTaskPrefix())));

    Map<String, Set<DatastreamTask>> assignment = Collections.emptyMap();
    List<String> instances = new ArrayList<>();
    instances.add("instance1");

    // Assign tasks for 1 datastream to 1 instance. Validate minTasks number of tasks are created.
    assignment = strategy.assign(datastreams, instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), minTasks);
    setupTaskLockForAssignment(assignment);

    List<String> partitions = ImmutableList.of("t-0", "t-1");
    DatastreamGroupPartitionsMetadata partitionsMetadata =
        new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    // Partition assignment should pass since we have enough tasks for the given partitions.
    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    Assert.assertEquals(assignment.get("instance1").size(), minTasks);
    int maxPartitionsPerTask = partitionsPerTask * fullnessFactorPct / 100;
    validatePartitionAssignment(assignment, partitions, maxPartitionsPerTask, minTasks);

    MetricsTestUtils.verifyMetrics(strategy, DynamicMetricsManager.getInstance());
    Gauge<?> gauge = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, datastreams.get(0).getName(), NUM_TASKS));
    Assert.assertEquals(gauge.getValue(), minTasks);
    gauge = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, datastreams.get(0).getName(), ELASTIC_TASK_PARAMETERS_NEED_ADJUSTMENT));
    Assert.assertEquals(gauge.getValue(), 0.0);
  }

  @Test
  public void testElasticTaskPartitionAssignmentCreatesAtMostMaxTasks() {
    int minTasks = 3;
    int maxTasks = 5;
    int partitionsPerTask = 2;
    int fullnessFactorPct = 50;
    // Create a strategy with partitionsPerTask = 2 and fullnessFactorPct as 50%
    StickyPartitionAssignmentStrategy strategy =
        createStickyPartitionAssignmentStrategy(partitionsPerTask, fullnessFactorPct, true, _zkClient, _clusterName);

    List<DatastreamGroup> datastreams = generateDatastreams("testElasticTaskPartitionAssignmentCreatesAtMostMaxTasks", 1, minTasks);
    datastreams.forEach(datastreamGroup -> {
      datastreamGroup.getDatastreams().get(0).getMetadata()
          .put(BroadcastStrategyFactory.CFG_MAX_TASKS, String.valueOf(maxTasks));
      _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, datastreamGroup.getTaskPrefix()));
    });

    Map<String, Set<DatastreamTask>> assignment = Collections.emptyMap();
    List<String> instances = new ArrayList<>();
    instances.add("instance1");

    // Assign tasks for 1 datastream to 1 instance. Validate minTasks number of tasks are created.
    assignment = strategy.assign(datastreams, instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), minTasks);
    setupTaskLockForAssignment(assignment);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0", "t1-1", "t2-0", "t2-1", "t3-0", "t3-1", "t4-0",
        "t4-1", "t5-0", "t5-1", "t6-1");
    DatastreamGroupPartitionsMetadata partitionsMetadata =
        new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    // Partition assignment should fail because insufficient tasks are present to fit the partitions such that the
    // tasks are 50% full. This should set the correct number of tasks needed so that the next assign() call creates
    // the correct number of tasks.
    Map<String, Set<DatastreamTask>> finalAssignment = assignment;
    Assert.assertThrows(DatastreamRuntimeException.class,
        () -> strategy.assignPartitions(finalAssignment, partitionsMetadata));

    // The assign call should create the only maxTasks number of tasks as set by partition assignment rather than the
    // expected number of tasks.
    assignment = strategy.assign(datastreams, instances, assignment);
    int maxPartitionsPerTask = partitionsPerTask * fullnessFactorPct / 100;
    int numTasksNeeded = (partitions.size() / maxPartitionsPerTask)
        + (partitions.size() % maxPartitionsPerTask == 0 ? 0 : 1);
    Assert.assertEquals(assignment.get("instance1").size(), maxTasks);
    Assert.assertTrue(numTasksNeeded > maxTasks);
    setupTaskLockForAssignment(assignment);

    // Partition assignment should go through
    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    maxPartitionsPerTask = partitions.size() / maxTasks  + (partitions.size() % maxTasks == 0 ? 0 : 1);
    validatePartitionAssignment(assignment, partitions, maxPartitionsPerTask, maxTasks);

    MetricsTestUtils.verifyMetrics(strategy, DynamicMetricsManager.getInstance());
    Gauge<?> gauge = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, datastreams.get(0).getName(), NUM_TASKS));
    Assert.assertEquals(gauge.getValue(), maxTasks);
    gauge = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, datastreams.get(0).getName(), ELASTIC_TASK_PARAMETERS_NEED_ADJUSTMENT));
    Assert.assertEquals(gauge.getValue(), 1.0);
  }

  @Test
  public void testNegativeMinTasks() {
    int minTasks = -3;
    int maxTasks = 5;
    int partitionsPerTask = 2;
    int fullnessFactorPct = 50;
    // Create a strategy with partitionsPerTask = 2 and fullnessFactorPct as 50%
    StickyPartitionAssignmentStrategy strategy =
        createStickyPartitionAssignmentStrategy(partitionsPerTask, fullnessFactorPct, true, _zkClient, _clusterName);

    List<DatastreamGroup> datastreams = generateDatastreams("ds", 1, minTasks);
    datastreams.forEach(datastreamGroup -> {
      datastreamGroup.getDatastreams().get(0).getMetadata()
          .put(BroadcastStrategyFactory.CFG_MAX_TASKS, String.valueOf(maxTasks));
      _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, datastreamGroup.getTaskPrefix()));
    });

    Map<String, Set<DatastreamTask>> assignment = Collections.emptyMap();
    List<String> instances = new ArrayList<>();
    instances.add("instance1");

    // Assign tasks for 1 datastream to 1 instance. Validate maxTasks number of tasks are created, since negative
    // minTasks should disable elastic task assignment for this datastream group.
    assignment = strategy.assign(datastreams, instances, assignment);
    Assert.assertEquals(assignment.get("instance1").size(), maxTasks);
    setupTaskLockForAssignment(assignment);

    List<String> partitions = ImmutableList.of("t-0", "t-1", "t1-0", "t1-1", "t2-0", "t2-1", "t3-0", "t3-1", "t4-0",
        "t4-1", "t5-0", "t5-1", "t6-1");
    DatastreamGroupPartitionsMetadata partitionsMetadata =
        new DatastreamGroupPartitionsMetadata(datastreams.get(0), partitions);

    // Partition assignment should pass, since elastic task assignment is disabled for a datastream group with only
    // negative minTasks
    assignment = strategy.assignPartitions(assignment, partitionsMetadata);
    Assert.assertEquals(assignment.get("instance1").size(), maxTasks);

    MetricsTestUtils.verifyMetrics(strategy, DynamicMetricsManager.getInstance());
  }

  @Test
  public void testZkClientWithElasticTaskEnabledConfigMismatch() {
    int partitionsPerTask = 2;
    int fullnessFactorPct = 50;

    // Enable elastic task assignment, and pass an empty ZkClient. This should throw.
    Assert.assertThrows(IllegalArgumentException.class, () -> createStickyPartitionAssignmentStrategy(partitionsPerTask,
        fullnessFactorPct, true, null, _clusterName));

    // Disable elastic task assignment, and pass valid ZkClient. This should not throw.
    createStickyPartitionAssignmentStrategyObject(partitionsPerTask, fullnessFactorPct, _zkClient, _clusterName);

    // Disable elastic task assignment, and pass an empty ZkClient. This should not throw.
    createStickyPartitionAssignmentStrategyObject(partitionsPerTask, fullnessFactorPct, null, _clusterName);
  }

  @Test
  public void testClusterNameWithElasticTaskEnabledConfigMismatch() {
    int partitionsPerTask = 2;
    int fullnessFactorPct = 50;

    // Pass null or blank clusterName while elastic task assignment is enabled. This should throw.
    Assert.assertThrows(IllegalArgumentException.class, () -> createStickyPartitionAssignmentStrategy(partitionsPerTask,
        fullnessFactorPct, true, _zkClient, null));

    Assert.assertThrows(IllegalArgumentException.class, () -> createStickyPartitionAssignmentStrategy(partitionsPerTask,
        fullnessFactorPct, true, _zkClient, ""));

    // If elastic task assignment is disabled, it should not matter whether clusterName is null or has a valid value.
    createStickyPartitionAssignmentStrategyObject(0, 0, null, null);

    createStickyPartitionAssignmentStrategyObject(0, 0, null, _clusterName);
  }

  @Test
  public void testExpectedNumberOfTasks() {
    StickyPartitionAssignmentStrategy strategy = createStickyPartitionAssignmentStrategy(3, 90, true,
        getZkClient(true), _clusterName);

    List<DatastreamGroup> ds = generateDatastreams("test", 1, 5);
    _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, ds.get(0).getName()));
    int numTasks = strategy.constructExpectedNumberOfTasks(ds.get(0), 3);
    Assert.assertEquals(numTasks, 5);
    Assert.assertEquals(numTasks, getNumTasksForDatastreamFromZK(ds.get(0).getName()));

    ds = generateDatastreams("test1", 1);
    _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, ds.get(0).getName()));
    numTasks = strategy.constructExpectedNumberOfTasks(ds.get(0), 3);
    Assert.assertEquals(numTasks, 3);
    Assert.assertEquals(-1, getNumTasksForDatastreamFromZK(ds.get(0).getName()));
  }

  private int getNumTasksForDatastreamFromZK(String taskPrefix) {
    String numTasksPath = KeyBuilder.datastreamNumTasks(_clusterName, taskPrefix);
    if (!_zkClient.exists(numTasksPath)) {
      return -1;
    }
    return Integer.parseInt(_zkClient.readData(numTasksPath));
  }

  private void createStickyPartitionAssignmentStrategyObject(int partitionsPerTask, int partitionFullnessFactorPct,
      ZkClient zkClient, String clusterName) {
    createStickyPartitionAssignmentStrategy(partitionsPerTask, partitionFullnessFactorPct, false, zkClient,
        clusterName);
  }

  @NotNull
  private StickyPartitionAssignmentStrategy createStickyPartitionAssignmentStrategy(int partitionsPerTask,
      int fullnessFactorPct, boolean enableElasticTask, ZkClient zkClient, String clusterName) {
    return new StickyPartitionAssignmentStrategy(Optional.empty(), DEFAULT_IMBALANCE_THRESHOLD, Integer.MAX_VALUE, enableElasticTask,
        partitionsPerTask, fullnessFactorPct, zkClient, clusterName);
  }

  private void setupTaskLockForAssignment(Map<String, Set<DatastreamTask>> assignment) {
    for (String instance : assignment.keySet()) {
      for (DatastreamTask task : assignment.get(instance)) {
        ZkAdapter mockZkAdapter = mock(ZkAdapter.class);
        ((DatastreamTaskImpl) task).setZkAdapter(mockZkAdapter);
        when(mockZkAdapter.checkIsTaskLocked(anyString(), anyString(), anyString())).thenReturn(true);
      }
    }
  }

  private void validatePartitionAssignment(Map<String, Set<DatastreamTask>> assignment, List<String> partitions,
      int maxPartitionsPerTask, int numTasksNeeded) {
    Assert.assertEquals(assignment.get("instance1").size(), numTasksNeeded);

    int numPartitions = 0;
    Set<String> partitionsFound = new HashSet<>();
    for (DatastreamTask task : assignment.get("instance1")) {
      Assert.assertTrue(task.getPartitionsV2().size() <= maxPartitionsPerTask);
      numPartitions += task.getPartitionsV2().size();
      partitionsFound.addAll(task.getPartitionsV2());
    }
    Assert.assertEquals(numPartitions, partitions.size());
    Assert.assertEquals(new HashSet<>(partitions), partitionsFound);
  }

  private  Map<String, Set<DatastreamTask>> generateEmptyAssignment(List<DatastreamGroup> datastreams,
      int instanceNum, int taskNum, boolean isTaskLocked) {
    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();
    for (int i = 0; i < instanceNum; ++i) {
      Set<DatastreamTask> set = new HashSet<>();
      for (int j = 0; j < taskNum; ++j) {
        DatastreamTaskImpl task = new DatastreamTaskImpl(datastreams.get(0).getDatastreams());
        ZkAdapter mockZkAdapter = mock(ZkAdapter.class);
        task.setZkAdapter(mockZkAdapter);
        when(mockZkAdapter.checkIsTaskLocked(anyString(), anyString(), anyString())).thenReturn(isTaskLocked);
        set.add(task);
      }
      assignment.put("instance" + i, set);
    }
    return assignment;
  }

  private Set<String> getPartitionsFromTask(Set<DatastreamTask> tasks) {
    Set<String> partitions = new HashSet<>();
    tasks.forEach(t -> partitions.addAll(t.getPartitionsV2()));
    return partitions;
  }

  private int getTotalPartitions(Map<String, Set<DatastreamTask>> assignment) {
    int count = 0;
    for (Set<DatastreamTask> tasks : assignment.values()) {
      count += tasks.stream().map(t -> t.getPartitionsV2().size()).mapToInt(Integer::intValue).sum();
    }
    return count;
  }

  private List<DatastreamGroup> generateDatastreams(String namePrefix, int numberOfDatastreams) {
    return generateDatastreams(namePrefix, numberOfDatastreams, 0);
  }

  private List<DatastreamGroup> generateDatastreams(String namePrefix, int numberOfDatastreams, int minTasks) {
    List<DatastreamGroup> datastreams = new ArrayList<>();
    String type = DummyConnector.CONNECTOR_TYPE;
    for (int index = 0; index < numberOfDatastreams; index++) {
      Datastream ds = DatastreamTestUtils.createDatastream(type, namePrefix + index, "DummySource");
      ds.getMetadata().put(DatastreamMetadataConstants.OWNER_KEY, "person_" + index);
      ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds));
      if (minTasks != 0) {
        ds.getMetadata().put(StickyPartitionAssignmentStrategy.CFG_MIN_TASKS, String.valueOf(minTasks));
      }
      datastreams.add(new DatastreamGroup(Collections.singletonList(ds)));
    }
    return datastreams;
  }

  private ZkClient getZkClient(boolean enableElasticTaskAssignment) {
    return enableElasticTaskAssignment ? _zkClient : null;
  }
}
