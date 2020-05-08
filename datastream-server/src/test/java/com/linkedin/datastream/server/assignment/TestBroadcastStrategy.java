/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;


/**
 * Tests for {@link BroadcastStrategy}
 */
public class TestBroadcastStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(TestBroadcastStrategy.class.getName());

  @Test
  public void testCreatesAssignmentAcrossAllInstances() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), datastreams.size());
    }

    // test with broadcast strategy where max tasks is not capped by instances size
    int maxTasksConfig = 12;
    BroadcastStrategy unlimitedStrategy = new BroadcastStrategy(Optional.of(maxTasksConfig));
    assignment = unlimitedStrategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    int expected = datastreams.size() * maxTasksConfig / instances.length;
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), expected);
    }
  }

  @Test
  public void testMaxTasks() {
    int numDatastreams = 10;
    int numInstances = 20;
    int maxTasks = 7;
    int expectedTotalTasks = numDatastreams * maxTasks;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    doTestMaxTasks(new BroadcastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);

    // test with broadcast strategy where max tasks is not capped by instances size
    numInstances = 7;
    maxTasks = 20;
    expectedTotalTasks = numDatastreams * maxTasks;
    doTestMaxTasks(new BroadcastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);
  }

  @Test
  public void testMaxTasksDatastreamOverride() {
    int numDatastreams = 25;
    int numInstances = 32;
    int maxTasks = 6;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    datastreams.get(0).getDatastreams().get(0).getMetadata().put(CFG_MAX_TASKS, "18");
    int expectedTotalTasks = numDatastreams * maxTasks + (18 - maxTasks);
    doTestMaxTasks(new BroadcastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);

    // test with broadcast strategy where max tasks is not capped by instances size
    numInstances = 6;
    maxTasks = 32;
    expectedTotalTasks = numDatastreams * maxTasks - (maxTasks - 18);
    doTestMaxTasks(new BroadcastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);
  }

  private void doTestMaxTasks(BroadcastStrategy strategy, int numInstances, int expectedTotalTasks,
      List<DatastreamGroup> datastreams) {
    String[] instances = IntStream.range(0, numInstances).mapToObj(x -> "instance" + x).toArray(String[]::new);
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    int taskPerInstances = (int) Math.ceil((double) expectedTotalTasks / numInstances);
    int totalTasks = 0;
    for (String instance : instances) {
      Assert.assertTrue(assignment.get(instance).size() <= taskPerInstances);
      totalTasks += assignment.get(instance).size();
    }
    Assert.assertEquals(totalTasks, expectedTotalTasks);
  }

  private List<DatastreamGroup> generateDatastreams(String namePrefix, int numberOfDatastreams) {
    List<DatastreamGroup> datastreams = new ArrayList<>();
    String type = DummyConnector.CONNECTOR_TYPE;
    for (int index = 0; index < numberOfDatastreams; index++) {
      Datastream ds = DatastreamTestUtils.createDatastream(type, namePrefix + index, "DummySource");
      ds.getMetadata().put(DatastreamMetadataConstants.OWNER_KEY, "person_" + index);
      ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds));
      datastreams.add(new DatastreamGroup(Collections.singletonList(ds)));
    }
    return datastreams;
  }

  @Test
  public void testDontCreateNewTasksWhenCalledSecondTime() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size(), newAssignmentTasks.size());
      LOG.info("New assignment : " + newAssignmentTasks);
      LOG.info("Old assignment : " + oldAssignmentTasks);
      Assert.assertTrue(newAssignmentTasks.containsAll(oldAssignmentTasks));
    }

    // test with broadcast strategy where max tasks is not capped by instances size
    int maxTasksConfig = 12;
    strategy = new BroadcastStrategy(Optional.of(maxTasksConfig));
    assignment = strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size(), newAssignmentTasks.size());
      LOG.info("New assignment : " + newAssignmentTasks);
      LOG.info("Old assignment : " + oldAssignmentTasks);
      Assert.assertTrue(newAssignmentTasks.containsAll(oldAssignmentTasks));
    }
  }

  @Test
  public void testRemoveDatastreamTasksWhenDatastreamIsDeleted() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());

    datastreams.remove(0);

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() - 1, newAssignmentTasks.size());
    }

    // test with broadcast strategy where max tasks is not capped by instances size
    int maxTasksConfig = 12;
    datastreams = generateDatastreams("ds", 5);
    strategy = new BroadcastStrategy(Optional.of(maxTasksConfig));
    assignment = strategy.assign(datastreams, instances, new HashMap<>());

    datastreams.remove(0);

    newAssignment = strategy.assign(datastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() - (maxTasksConfig / instances.size()),
          newAssignmentTasks.size());
    }
  }

  @Test
  public void testCreateNewTasksOnlyForNewDatastreamWhenDatastreamIsCreated() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());

    List<DatastreamGroup> newDatastreams = new ArrayList<>(datastreams);
    DatastreamGroup newDatastream = generateDatastreams("newds", 1).get(0);
    newDatastreams.add(newDatastream);

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(newDatastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() + 1, newAssignmentTasks.size());
      Assert.assertTrue(oldAssignmentTasks.stream()
          .allMatch(x -> x.getTaskPrefix().equals(newDatastream.getTaskPrefix()) || newAssignmentTasks.contains(x)));
    }

    // test with broadcast strategy where max tasks is not capped by instances size
    int maxTasksConfig = 12;
    datastreams = generateDatastreams("ds", 5);
    strategy = new BroadcastStrategy(Optional.of(maxTasksConfig));
    assignment = strategy.assign(datastreams, instances, new HashMap<>());

    newDatastreams = new ArrayList<>(datastreams);
    DatastreamGroup newDatastream1 = generateDatastreams("newds", 1).get(0);
    newDatastreams.add(newDatastream1);

    newAssignment = strategy.assign(newDatastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() + (maxTasksConfig / instances.size()),
          newAssignmentTasks.size());
      Assert.assertTrue(oldAssignmentTasks.stream()
          .allMatch(x -> x.getTaskPrefix().equals(newDatastream1.getTaskPrefix()) || newAssignmentTasks.contains(x)));
    }
  }

  @Test
  public void testCreateNewTasksOnlyForNewInstanceWhenInstanceIsAdded() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    String instance4 = "instance4";
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());
    List<String> newInstances = new ArrayList<>(instances);
    newInstances.add(instance4);
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, newInstances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size(), newAssignmentTasks.size());
      Assert.assertTrue(newAssignmentTasks.containsAll(oldAssignmentTasks));
    }

    Assert.assertEquals(newAssignment.get(instance4).size(), datastreams.size());
  }

  @Test
  public void testRebalanceTasksWhenNewInstanceIsAdded() {
    // test that a rebalance occurs (redistribution of tasks) when a new instance is added, and broadcast strategy
    // where num tasks is not limited by instance size is being used
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    String instance4 = "instance4";
    int maxTasksConfig = 12;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy(Optional.of(maxTasksConfig));
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());
    List<String> newInstances = new ArrayList<>(instances);
    newInstances.add(instance4);
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, newInstances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    int expectedNumTasksPerInstance = maxTasksConfig * datastreams.size() / newInstances.size();
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(newAssignmentTasks.size(), expectedNumTasksPerInstance);
      Assert.assertTrue(oldAssignmentTasks.containsAll(newAssignmentTasks));
    }

    Assert.assertEquals(newAssignment.get(instance4).size(), expectedNumTasksPerInstance);
  }

  @Test
  public void testDontCreateNewTasksWhenInstanceChanged() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    instances = new String[]{"instance1", "instance2", "instance4"};

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);
    Set<DatastreamTask> oldAssignmentTasks = assignment.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    Set<DatastreamTask> newAssignmentTasks = newAssignment.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    Assert.assertEquals(oldAssignmentTasks, newAssignmentTasks);
    // make sure no task is assigned to more than one instance
    int totalTasks = newAssignment.values().stream().mapToInt(Set::size).sum();
    Assert.assertEquals(totalTasks, newAssignmentTasks.size());
  }
}
