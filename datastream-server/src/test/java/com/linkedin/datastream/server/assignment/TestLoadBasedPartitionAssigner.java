/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.PartitionThroughputInfo;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static org.mockito.Matchers.anyString;


/**
 * Tests for {@link LoadBasedPartitionAssigner}
 */
public class TestLoadBasedPartitionAssigner {
  /**
   * Test setup
   */
  @BeforeClass
  public void setup() {
    DynamicMetricsManager.createInstance(new MetricRegistry(), "TestLoadBasedPartitionAssigner");
  }

  @Test
  public void assignFromScratchTest() {
    List<String> unassignedPartitions = Arrays.asList("P1", "P2", "P3");
    ClusterThroughputInfo throughputInfo = getDummyClusterThroughputInfo(unassignedPartitions);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));
    currentAssignment.put("instance2", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));
    currentAssignment.put("instance3", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds1)), Arrays.asList("P1", "P2", "P3"));

    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();
    Map<String, Set<DatastreamTask>> newAssignment =
        assigner.assignPartitions(throughputInfo, currentAssignment, unassignedPartitions, metadata, Integer.MAX_VALUE);

    DatastreamTask task1 = (DatastreamTask) newAssignment.get("instance1").toArray()[0];
    DatastreamTask task2 = (DatastreamTask) newAssignment.get("instance2").toArray()[0];
    DatastreamTask task3 = (DatastreamTask) newAssignment.get("instance3").toArray()[0];

    // verify that tasks got 1 partition each
    Assert.assertEquals(task1.getPartitionsV2().size(), 1);
    Assert.assertEquals(task2.getPartitionsV2().size(), 1);
    Assert.assertEquals(task3.getPartitionsV2().size(), 1);

    // verify that all partitions got assigned
    Set<String> assignedPartitions = new HashSet<>();
    assignedPartitions.addAll(task1.getPartitionsV2());
    assignedPartitions.addAll(task2.getPartitionsV2());
    assignedPartitions.addAll(task3.getPartitionsV2());
    Assert.assertTrue(assignedPartitions.contains("P1"));
    Assert.assertTrue(assignedPartitions.contains("P2"));
    Assert.assertTrue(assignedPartitions.contains("P3"));

    String stats = (((DatastreamTaskImpl) task1).getStats());
    LoadBasedPartitionAssigner.PartitionAssignmentStatPerTask statObj = LoadBasedPartitionAssigner.PartitionAssignmentStatPerTask.fromJson(stats);
    Assert.assertTrue(statObj.getIsThroughputRateLatest());
    Assert.assertEquals(statObj.getTotalPartitions(), 1);
    Assert.assertEquals(statObj.getPartitionsWithUnknownThroughput(), 0);
    Assert.assertEquals(statObj.getThroughputRateInKBps(), 5);
  }

  @Test
  public void newAssignmentRetainsTasksFromOtherDatastreamsTest() {
    List<String> assignedPartitions = Arrays.asList("P1", "P2");
    List<String> unassignedPartitions = Collections.singletonList("P3");
    List<String> allPartitions = new ArrayList<>(assignedPartitions);
    allPartitions.addAll(unassignedPartitions);
    ClusterThroughputInfo throughputInfo = getDummyClusterThroughputInfo(allPartitions);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(2);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));

    Datastream ds2 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds2")[0];
    ds2.getSource().setPartitions(0);
    ds2.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds2));

    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    DatastreamTask datastream1Task1 = createTaskForDatastream(ds1, Collections.singletonList("P1"));
    DatastreamTask datastream1Task2 = createTaskForDatastream(ds1, Collections.singletonList("P2"));
    DatastreamTask datastream2Task1 = createTaskForDatastream(ds2);
    currentAssignment.put("instance1", new HashSet<>(Arrays.asList(datastream1Task1, datastream2Task1)));
    currentAssignment.put("instance2", new HashSet<>(Collections.singletonList(datastream1Task2)));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds2)), Arrays.asList("P3"));

    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();
    Map<String, Set<DatastreamTask>> newAssignment = assigner.assignPartitions(throughputInfo, currentAssignment,
        unassignedPartitions, metadata, Integer.MAX_VALUE);

    Set<DatastreamTask> allTasks = new HashSet<>();
    allTasks.add((DatastreamTask) newAssignment.get("instance1").toArray()[0]);
    allTasks.add((DatastreamTask) newAssignment.get("instance1").toArray()[1]);
    allTasks.add((DatastreamTask) newAssignment.get("instance2").toArray()[0]);

    // verify that assignments for ds1 are retained
    Assert.assertTrue(allTasks.contains(datastream1Task1));
    Assert.assertTrue(allTasks.contains(datastream1Task2));

    // verify that P3 is assigned
    allTasks.remove(datastream1Task1);
    allTasks.remove(datastream1Task2);
    DatastreamTask newTask = (DatastreamTask) allTasks.toArray()[0];
    Assert.assertEquals(newTask.getPartitionsV2().size(), 1);
    Assert.assertTrue(newTask.getPartitionsV2().contains("P3"));

    String stats = (((DatastreamTaskImpl) newTask).getStats());
    LoadBasedPartitionAssigner.PartitionAssignmentStatPerTask statObj = LoadBasedPartitionAssigner.PartitionAssignmentStatPerTask.fromJson(stats);
    Assert.assertTrue(statObj.getIsThroughputRateLatest());
    Assert.assertEquals(statObj.getTotalPartitions(), 1);
    Assert.assertEquals(statObj.getPartitionsWithUnknownThroughput(), 0);
    Assert.assertEquals(statObj.getThroughputRateInKBps(), 5);
  }

  @Test
  public void assignmentDistributesPartitionsWhenThroughputInfoIsMissingTest() {
    // this tests the round-robin assignment of partitions that don't have throughput info
    List<String> unassignedPartitions = Arrays.asList("P1", "P2", "P3", "P4");
    ClusterThroughputInfo throughputInfo = new ClusterThroughputInfo("dummy", new HashMap<>());

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));
    currentAssignment.put("instance2", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds1)), Arrays.asList("P1", "P2", "P3", "P4"));

    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();
    Map<String, Set<DatastreamTask>> newAssignment = assigner.assignPartitions(throughputInfo, currentAssignment,
        unassignedPartitions, metadata, Integer.MAX_VALUE);

    DatastreamTask task1 = (DatastreamTask) newAssignment.get("instance1").toArray()[0];
    DatastreamTask task2 = (DatastreamTask) newAssignment.get("instance2").toArray()[0];

    // verify that tasks got 2 partition each
    Assert.assertEquals(task1.getPartitionsV2().size(), 2);
    Assert.assertEquals(task2.getPartitionsV2().size(), 2);

    String stats = (((DatastreamTaskImpl) task1).getStats());
    LoadBasedPartitionAssigner.PartitionAssignmentStatPerTask statObj = LoadBasedPartitionAssigner.PartitionAssignmentStatPerTask.fromJson(stats);
    Assert.assertFalse(statObj.getIsThroughputRateLatest());
    Assert.assertEquals(statObj.getTotalPartitions(), 2);
    Assert.assertEquals(statObj.getPartitionsWithUnknownThroughput(), 2);
    Assert.assertEquals(statObj.getThroughputRateInKBps(), 0);
  }

  @Test
  public void lightestTaskGetsNewPartitionTest() {
    List<String> unassignedPartitions = Arrays.asList("P4");
    Map<String, PartitionThroughputInfo> throughputInfoMap = new HashMap<>();
    throughputInfoMap.put("P1", new PartitionThroughputInfo(5, 5, "P1"));
    throughputInfoMap.put("P2", new PartitionThroughputInfo(5, 5, "P2"));
    throughputInfoMap.put("P3", new PartitionThroughputInfo(50, 5, "P3"));
    throughputInfoMap.put("P4", new PartitionThroughputInfo(20, 5, "P4"));
    ClusterThroughputInfo throughputInfo = new ClusterThroughputInfo("dummy", throughputInfoMap);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    DatastreamTask task1 = createTaskForDatastream(ds1, Arrays.asList("P1", "P2"));
    DatastreamTask task2 = createTaskForDatastream(ds1, Collections.singletonList("P3"));
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(task1)));
    currentAssignment.put("instance2", new HashSet<>(Collections.singletonList(task2)));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds1)), Arrays.asList("P1", "P2", "P3", "P4"));

    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();
    Map<String, Set<DatastreamTask>> newAssignment = assigner.assignPartitions(throughputInfo, currentAssignment,
        unassignedPartitions, metadata, Integer.MAX_VALUE);

    DatastreamTask task3 = (DatastreamTask) newAssignment.get("instance1").toArray()[0];

    // verify that task in instance1 got the new partition
    Assert.assertEquals(task3.getPartitionsV2().size(), 3);
    Assert.assertTrue(task3.getPartitionsV2().contains("P4"));
  }

  @Test
  public void throwsExceptionWhenNotEnoughRoomForAllPartitionsTest() {
    List<String> unassignedPartitions = Arrays.asList("P4", "P5");
    Map<String, PartitionThroughputInfo> throughputInfoMap = new HashMap<>();
    ClusterThroughputInfo throughputInfo = new ClusterThroughputInfo("dummy", throughputInfoMap);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    DatastreamTask task1 = createTaskForDatastream(ds1, Arrays.asList("P1", "P2"));
    DatastreamTask task2 = createTaskForDatastream(ds1, Collections.singletonList("P3"));
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(task1)));
    currentAssignment.put("instance2", new HashSet<>(Collections.singletonList(task2)));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds1)), Arrays.asList("P1", "P2", "P3", "P4", "P5"));

    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();
    int maxPartitionsPerTask = 2;
    Assert.assertThrows(DatastreamRuntimeException.class, () -> assigner.assignPartitions(throughputInfo,
        currentAssignment, unassignedPartitions, metadata, maxPartitionsPerTask));
  }

  @Test
  public void taskWithRoomGetsNewPartitionTest() {
    List<String> unassignedPartitions = Arrays.asList("P4");
    Map<String, PartitionThroughputInfo> throughputInfoMap = new HashMap<>();
    throughputInfoMap.put("P1", new PartitionThroughputInfo(5, 5, "P1"));
    throughputInfoMap.put("P2", new PartitionThroughputInfo(5, 5, "P2"));
    throughputInfoMap.put("P3", new PartitionThroughputInfo(50, 5, "P3"));
    throughputInfoMap.put("P4", new PartitionThroughputInfo(20, 5, "P4"));
    ClusterThroughputInfo throughputInfo = new ClusterThroughputInfo("dummy", throughputInfoMap);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    DatastreamTask task1 = createTaskForDatastream(ds1, Arrays.asList("P1", "P2"));
    DatastreamTask task2 = createTaskForDatastream(ds1, Collections.singletonList("P3"));
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(task1)));
    currentAssignment.put("instance2", new HashSet<>(Collections.singletonList(task2)));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds1)), Arrays.asList("P1", "P2", "P3", "P4"));

    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();
    int maxPartitionsPerTask = 2;
    Map<String, Set<DatastreamTask>> newAssignment = assigner.assignPartitions(throughputInfo, currentAssignment,
        unassignedPartitions, metadata, maxPartitionsPerTask);

    DatastreamTask task3 = (DatastreamTask) newAssignment.get("instance2").toArray()[0];

    // verify that task in instance2 got the new partition
    Assert.assertEquals(task3.getPartitionsV2().size(), 2);
    Assert.assertTrue(task3.getPartitionsV2().contains("P4"));
  }

  @Test
  public void findTaskWithRoomForAPartitionTests() {
    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();
    List<String> tasks = Arrays.asList("T1", "T2");
    Map<String, Set<String>> partitionsMap = new HashMap<>();
    partitionsMap.put("T1", new HashSet<>(Collections.emptySet()));
    partitionsMap.put("T2", new HashSet<>(Collections.emptySet()));
    int index = assigner.findTaskWithRoomForAPartition(tasks, partitionsMap, 0, 1);
    Assert.assertEquals(index, 0);
    partitionsMap.get("T1").add("P1");
    index = assigner.findTaskWithRoomForAPartition(tasks, partitionsMap, 0, 1);
    // no more room in T1, expecting 1 as index
    Assert.assertEquals(index, 1);
    partitionsMap.get("T2").add("P2");
    Assert.assertThrows(DatastreamRuntimeException.class, () ->
        assigner.findTaskWithRoomForAPartition(tasks, partitionsMap, 0, 1));

    List<String> tasks2 = Arrays.asList("T1", "T2", "T3");
    Map<String, Set<String>> partitionsMap2 = new HashMap<>();
    partitionsMap2.put("T1", new HashSet<>(Collections.emptySet()));
    partitionsMap2.put("T2", new HashSet<>(Collections.singletonList("P1")));
    partitionsMap2.put("T3", new HashSet<>(Collections.emptySet()));
    int index2 = assigner.findTaskWithRoomForAPartition(tasks2, partitionsMap2, 1, 1);
    Assert.assertEquals(index2, 2);
    partitionsMap2.get("T3").add("P2");
    index2 = assigner.findTaskWithRoomForAPartition(tasks2, partitionsMap2, 1, 1);
    Assert.assertEquals(index2, 0);
  }

  private DatastreamTask createTaskForDatastream(Datastream datastream) {
    return createTaskForDatastream(datastream, Collections.emptyList());
  }

  private DatastreamTask createTaskForDatastream(Datastream datastream, List<String> partitions) {
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setPartitionsV2(partitions);
    ZkAdapter mockAdapter = Mockito.mock(ZkAdapter.class);
    Mockito.when(mockAdapter.checkIsTaskLocked(anyString(), anyString(), anyString())).thenReturn(true);
    task.setZkAdapter(mockAdapter);
    return task;
  }

  private ClusterThroughputInfo getDummyClusterThroughputInfo(List<String> partitions) {
    Map<String, PartitionThroughputInfo> partitionThroughputMap = new HashMap<>();
    for (String partitionName : partitions) {
      int bytesInRate = 5;
      int messagesInRate = 5;
      partitionThroughputMap.put(partitionName,
          new PartitionThroughputInfo(bytesInRate, messagesInRate, partitionName));
    }
    return new ClusterThroughputInfo("dummy", partitionThroughputMap);
  }
}
