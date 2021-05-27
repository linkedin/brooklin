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

    ArrayList<String> list1 = new ArrayList<>(Arrays.asList("P1", "P2", "P3"));
    String partitionToInsert1 = "P4";
    Map<String, Integer> throughputMap1 = new HashMap<>();
    throughputMap1.put("P1", 0);
    throughputMap1.put("P2", 0);
    throughputMap1.put("P3", 0);
    throughputMap1.put("P4", 0);
    assigner.insertTaskIntoSortedList(partitionToInsert1, list1, throughputMap1);
    Assert.assertEquals(list1.size(), 4);
    Assert.assertTrue(list1.contains(partitionToInsert1));

    ArrayList<String> list2 = new ArrayList<>(Arrays.asList("P1", "P2"));
    String partitionToInsert2 = "P3";
    Map<String, Integer> throughputMap2 = new HashMap<>();
    throughputMap2.put("P1", 0);
    throughputMap2.put("P2", 1);
    throughputMap2.put("P3", 2);
    assigner.insertTaskIntoSortedList(partitionToInsert2, list2, throughputMap2);
    Assert.assertEquals(list2.size(), 3);
    Assert.assertTrue(list2.contains(partitionToInsert2));
    Assert.assertTrue(list2.get(list2.size() - 1).equals(partitionToInsert2));

    ArrayList<String> list3 = new ArrayList<>(Arrays.asList("P1", "P2"));
    String partitionToInsert3 = "P3";
    Map<String, Integer> throughputMap3 = new HashMap<>();
    throughputMap3.put("P1", 2);
    throughputMap3.put("P2", 3);
    throughputMap3.put("P3", 1);
    assigner.insertTaskIntoSortedList(partitionToInsert3, list3, throughputMap3);
    Assert.assertEquals(list3.size(), 3);
    Assert.assertTrue(list3.contains(partitionToInsert3));
    Assert.assertTrue(list3.get(0).equals(partitionToInsert3));
  }
}
