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
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.connectors.DummyConnector;
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
        assigner.assignPartitions(throughputInfo, currentAssignment, unassignedPartitions, metadata);

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
    Map<String, Set<DatastreamTask>> newAssignment =
        assigner.assignPartitions(throughputInfo, currentAssignment, unassignedPartitions, metadata);

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
    Map<String, Set<DatastreamTask>> newAssignment =
        assigner.assignPartitions(throughputInfo, currentAssignment, unassignedPartitions, metadata);

    DatastreamTask task1 = (DatastreamTask) newAssignment.get("instance1").toArray()[0];
    DatastreamTask task2 = (DatastreamTask) newAssignment.get("instance2").toArray()[0];

    // verify that tasks got 2 partition each
    Assert.assertEquals(task1.getPartitionsV2().size(), 2);
    Assert.assertEquals(task2.getPartitionsV2().size(), 2);
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
    Map<String, Set<DatastreamTask>> newAssignment =
        assigner.assignPartitions(throughputInfo, currentAssignment, unassignedPartitions, metadata);

    DatastreamTask task3 = (DatastreamTask) newAssignment.get("instance1").toArray()[0];

    // verify that task in instance1 got the new partition
    Assert.assertEquals(task3.getPartitionsV2().size(), 3);
    Assert.assertTrue(task3.getPartitionsV2().contains("P4"));
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

  @Test
  public void insertionTests() {
    LoadBasedPartitionAssigner assigner = new LoadBasedPartitionAssigner();

    ArrayList<String> list1 = new ArrayList<>(Arrays.asList("T1", "T2", "T3"));
    String taskToInsert1 = "T4";
    Map<String, Integer> throughputMap1 = new HashMap<>();
    throughputMap1.put("T1", 0);
    throughputMap1.put("T2", 0);
    throughputMap1.put("T3", 0);
    throughputMap1.put("T4", 0);
    assigner.insertTaskIntoSortedList(taskToInsert1, list1, throughputMap1);
    Assert.assertEquals(list1.size(), 4);
    Assert.assertTrue(list1.contains(taskToInsert1));

    ArrayList<String> list2 = new ArrayList<>(Arrays.asList("T1", "T2"));
    String taskToInsert2 = "T3";
    Map<String, Integer> throughputMap2 = new HashMap<>();
    throughputMap2.put("T1", 0);
    throughputMap2.put("T2", 1);
    throughputMap2.put("T3", 2);
    assigner.insertTaskIntoSortedList(taskToInsert2, list2, throughputMap2);
    Assert.assertEquals(list2.size(), 3);
    Assert.assertTrue(list2.contains(taskToInsert2));
    Assert.assertEquals(taskToInsert2, list2.get(list2.size() - 1));

    ArrayList<String> list3 = new ArrayList<>(Arrays.asList("T1", "T2"));
    String taskToInsert3 = "T3";
    Map<String, Integer> throughputMap3 = new HashMap<>();
    throughputMap3.put("T1", 2);
    throughputMap3.put("T2", 3);
    throughputMap3.put("T3", 1);
    assigner.insertTaskIntoSortedList(taskToInsert3, list3, throughputMap3);
    Assert.assertEquals(list3.size(), 3);
    Assert.assertTrue(list3.contains(taskToInsert3));
    Assert.assertEquals(taskToInsert3, list3.get(0));

    ArrayList<String> list4 = new ArrayList<>(Collections.singletonList("T1"));
    String taskToInsert4 = "T2";
    Map<String, Integer> throughputMap4 = new HashMap<>();
    throughputMap4.put("T1", 1);
    throughputMap4.put("T2", 2);
    assigner.insertTaskIntoSortedList(taskToInsert4, list4, throughputMap4);
    Assert.assertEquals(list4.size(), 2);
    Assert.assertTrue(list4.contains(taskToInsert4));
    Assert.assertEquals(taskToInsert4, list4.get(1));

    ArrayList<String> list5 = new ArrayList<>();
    String taskToInsert5 = "T1";
    Map<String, Integer> throughputMap5 = new HashMap<>();
    throughputMap5.put("T1", 5);
    assigner.insertTaskIntoSortedList(taskToInsert5, list5, throughputMap5);
    Assert.assertEquals(list5.size(), 1);
    Assert.assertTrue(list5.contains(taskToInsert5));
    Assert.assertEquals(taskToInsert5, list5.get(0));
  }
}
