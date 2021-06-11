/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.jetbrains.annotations.NotNull;
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
 * Tests for {@link StickyMulticastStrategy}
 */
public class TestStickyMulticastStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(TestBroadcastStrategy.class.getName());

  @Test
  public void testCreateAssignmentAcrossAllInstances()  {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), datastreams.size());
    }

    // test with strategy where dsTaskLimitPerInstance is greater than 1
    int maxTasksConfig = 12;
    strategy = new StickyMulticastStrategy(Optional.of(maxTasksConfig));
    assignment = strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    int expected = datastreams.size() * maxTasksConfig / instances.length;
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), expected);
    }
  }

  @Test
  public void testCreateNewAssignmentRandomlyEachTime() {
    String[] instances = new String[]{"instance1", "instance2", "instance3", "instance4", "instance5"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds1", 3);
    List<DatastreamGroup> datastreams2 = generateDatastreams("ds2", 1);
    datastreams.get(0).getDatastreams().get(0).getMetadata().put(CFG_MAX_TASKS, "13");
    datastreams2.get(0).getDatastreams().get(0).getMetadata().put(CFG_MAX_TASKS, "13");

    StickyMulticastStrategy strategy = new StickyMulticastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> originalAssignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    System.out.println(originalAssignment);

    datastreams.addAll(datastreams2);

    Map<String, Set<DatastreamTask>> newAssignment1 =
        strategy.assign(datastreams, Arrays.asList(instances), originalAssignment);


    Map<String, Set<DatastreamTask>> newAssignment2 =
        strategy.assign(datastreams, Arrays.asList(instances), originalAssignment);


    List<Integer> assignment1Size = newAssignment1.values().stream().map(Set::size).collect(Collectors.toList());
    List<Integer> assignment2Size = newAssignment2.values().stream().map(Set::size).collect(Collectors.toList());

    //Since this is random shuffling, there is still a chance that we have the same results, we compute multiple times
    //to avoid the same assignment
    final int maxAttempts = 100;
    for (int i = 0; i < maxAttempts; ++i) {
      if (!assignment1Size.equals(assignment2Size)) {
        break;
      } else {
        newAssignment2 =
            strategy.assign(datastreams, Arrays.asList(instances), originalAssignment);
        assignment2Size = newAssignment2.values().stream().map(Set::size).collect(Collectors.toList());
      }
    }

    Assert.assertNotEquals(assignment1Size, assignment2Size);
  }


  @Test
  public void testMaxTasks() {
    int numDatastreams = 10;
    int numInstances = 20;
    int maxTasks = 7;
    int expectedTotalTasks = numDatastreams * maxTasks;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    doTestMaxTasks(new StickyMulticastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);

    // test with strategy where dsTaskLimitPerInstance is greater than 1
    numInstances = 7;
    maxTasks = 20;
    expectedTotalTasks = numDatastreams * maxTasks;
    doTestMaxTasks(new StickyMulticastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);
  }

  @Test
  public void testMaxTasksDatastreamOverride() {
    int numDatastreams = 25;
    int numInstances = 32;
    int maxTasks = 6;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    datastreams.get(0).getDatastreams().get(0).getMetadata().put(CFG_MAX_TASKS, "18");

    int expectedTotalTasks = numDatastreams * maxTasks + (18 - maxTasks);
    doTestMaxTasks(new StickyMulticastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);

    // test with strategy where dsTaskLimitPerInstance is greater than 1
    numInstances = 6;
    maxTasks = 32;
    expectedTotalTasks = numDatastreams * maxTasks - (maxTasks - 18);
    doTestMaxTasks(new StickyMulticastStrategy(Optional.of(maxTasks)), numInstances, expectedTotalTasks, datastreams);
  }


  private void doTestMaxTasks(StickyMulticastStrategy strategy, int numInstances, int expectedTotalTasks,
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

  @Test
  public void testEditDiff() {
    int numDatastreams = 11;
    int numInstances = 20;
    int maxTasks = 7;
    int expectedTotalTasks = numDatastreams * maxTasks;

    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    List<String> instances = IntStream.range(0, numInstances).mapToObj(x -> "instance" + x).collect(Collectors.toList());

    Map<String, Set<DatastreamTask>> assignment1 =
        new StickyMulticastStrategy(Optional.of(maxTasks)).assign(datastreams, instances, new HashMap<>());

    Assert.assertEquals(getFullTaskList(assignment1).size(), expectedTotalTasks);
    Assert.assertEquals(editDiff(assignment1, new HashMap<>()), expectedTotalTasks);

    // Test instance goes down
    Random rnd = new Random();
    String instanceDown = instances.remove(rnd.nextInt(instances.size()));
    int numTasksInInstanceDown = assignment1.get(instanceDown).size();

    Map<String, Set<DatastreamTask>> assignment2 =
        new StickyMulticastStrategy(Optional.of(maxTasks)).assign(datastreams, instances, assignment1);
    Assert.assertTrue(editDiff(assignment1, assignment2) / (numTasksInInstanceDown + 1) <= 2);

    // Test instance coming up
    instances.add(instanceDown);
    Map<String, Set<DatastreamTask>> assignment3 =
        new StickyMulticastStrategy(Optional.of(maxTasks)).assign(datastreams, instances, assignment2);
    int numTasksInNewInstance = assignment3.get(instanceDown).size();
    Assert.assertTrue(numTasksInNewInstance <= numTasksInInstanceDown);
    Assert.assertEquals(editDiff(assignment2, assignment3), numTasksInNewInstance * 2);
  }

  @Test
  public void testEditDiffWithLargeMaxTasks() {
    int numDatastreams = 2;
    int numInstances = 10;
    int maxTasks = 30;
    int expectedTotalTasks = numDatastreams * maxTasks;

    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    List<String> instances = IntStream.range(0, numInstances).mapToObj(x -> "instance" + x).collect(Collectors.toList());

    Map<String, Set<DatastreamTask>> assignment1 =
        new StickyMulticastStrategy(Optional.of(maxTasks)).assign(datastreams, instances, new HashMap<>());

    Assert.assertEquals(getFullTaskList(assignment1).size(), expectedTotalTasks);
    Assert.assertEquals(editDiff(assignment1, new HashMap<>()), expectedTotalTasks);

    // Test instance goes down
    Random rnd = new Random();
    String instanceDown = instances.remove(rnd.nextInt(instances.size()));
    int numTasksInInstanceDown = assignment1.get(instanceDown).size();

    Map<String, Set<DatastreamTask>> assignment2 =
        new StickyMulticastStrategy(Optional.of(maxTasks)).assign(datastreams, instances, assignment1);
    Assert.assertTrue(editDiff(assignment1, assignment2) / (numTasksInInstanceDown + 1) <= 2);

    // Test instance coming up
    instances.add(instanceDown);
    Map<String, Set<DatastreamTask>> assignment3 =
        new StickyMulticastStrategy(Optional.of(maxTasks)).assign(datastreams, instances, assignment2);
    int numTasksInNewInstance = assignment3.get(instanceDown).size();
    Assert.assertTrue(numTasksInNewInstance <= numTasksInInstanceDown);
    Assert.assertEquals(editDiff(assignment2, assignment3), numTasksInNewInstance * 2);
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
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(Optional.empty());
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

    // test with strategy where dsTaskLimitPerInstance is greater than 1
    strategy = new StickyMulticastStrategy(Optional.of(12));
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
  public void testDontCreateNewTasksWhenInstanceChanged() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    instances = new String[]{"instance1", "instance2"};

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);
    Set<DatastreamTask> oldAssignmentTasks = assignment.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    Set<DatastreamTask> newAssignmentTasks = newAssignment.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    Assert.assertTrue(oldAssignmentTasks.containsAll(newAssignmentTasks));
  }

  @Test
  public void testSameTaskIsNotAssignedToMoreThanOneInstance() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    int numDatastreams = 5;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    StickyMulticastStrategy strategy = createStickyMulticastStrategyObject(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    // Copying the assignment to simulate the scenario where two instances have the same task,
    // which is possible when the previous leader gets interrupted while updating the assignment.
    assignment.get("instance1").addAll(assignment.get("instance2"));

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);
    Set<DatastreamTask> newAssignmentTasks = newAssignment.values().stream().flatMap(Set::stream).collect(Collectors.toSet());
    List<DatastreamTask> newAssignmentTasksList = newAssignment.values().stream().flatMap(Set::stream).collect(Collectors.toList());
    Assert.assertEquals(newAssignmentTasks.size(), newAssignmentTasksList.size());
    Assert.assertEquals(newAssignmentTasks.size(), instances.length * numDatastreams);
  }

  @NotNull
  private StickyMulticastStrategy createStickyMulticastStrategyObject(Optional<Integer> maxTasks) {
    return new StickyMulticastStrategy(maxTasks);
  }

  @Test
  public void testRemoveDatastreamTasksWhenDatastreamIsDeleted() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = createStickyMulticastStrategyObject(Optional.empty());
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());

    datastreams.remove(0);

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() - 1, newAssignmentTasks.size());
    }

    // test with strategy where dsTaskLimitPerInstance is greater than 1
    int maxTasksConfig = 12;
    datastreams = generateDatastreams("ds", 5);
    strategy = createStickyMulticastStrategyObject(Optional.of(maxTasksConfig));
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
    StickyMulticastStrategy strategy = createStickyMulticastStrategyObject(Optional.empty());
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

    // test with strategy where dsTaskLimitPerInstance is greater than 1
    int maxTasksConfig = 12;
    datastreams = generateDatastreams("ds", 5);
    strategy = createStickyMulticastStrategyObject(Optional.of(maxTasksConfig));
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
    StickyMulticastStrategy strategy = createStickyMulticastStrategyObject(Optional.empty());
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
  public void testStickyFairDistributionWhenNewInstanceIsAdded() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    String instance4 = "instance4";
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    int maxTasksConfig = 12;
    StickyMulticastStrategy strategy = createStickyMulticastStrategyObject(Optional.of(maxTasksConfig));
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());
    List<String> newInstances = new ArrayList<>(instances);
    newInstances.add(instance4);
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, newInstances, assignment);

    // Ensure that the datastream tasks for the existing instances were sticky
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() - datastreams.size(), newAssignmentTasks.size());
      Assert.assertTrue(oldAssignmentTasks.containsAll(newAssignmentTasks));
    }

    Assert.assertEquals(newAssignment.get(instance4).size(),
        maxTasksConfig * datastreams.size() / newInstances.size());
  }

  @Test
  public void testDontRebalanceWhenDeletingDatastream() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    int imbalanceThreshold = 2;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    int maxTasksConfig = 4;
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(Optional.of(maxTasksConfig), 2);
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());
    // Remove some data streams to create a hole, it may or may not cause the imbalance, but rebalance shouldn't happen
    datastreams.remove(3);
    datastreams.remove(0);
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances were sticky
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertTrue(oldAssignmentTasks.containsAll(newAssignmentTasks));
    }

    List<String> instancesBySize = new ArrayList<>(instances);
    instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));
    Assert.assertTrue(newAssignment.get(instancesBySize.get(instances.size() - 1)).size() -
        newAssignment.get(instancesBySize.get(0)).size() <= imbalanceThreshold);
  }

  @Test
  public void testTriggerRebalanceWhenDeletingDatastream() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    int imbalanceThreshold = 1;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    int maxTasksConfig = 4;
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(Optional.of(maxTasksConfig), imbalanceThreshold);
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());
    // Remove some data streams to create a hole, causing an imbalance
    datastreams.remove(3);
    datastreams.remove(0);
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, instances, assignment);

    List<String> instancesBySize = new ArrayList<>(instances);
    instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));
    Assert.assertEquals(newAssignment.get(instancesBySize.get(0)).size(),
        newAssignment.get(instancesBySize.get(instances.size() - 1)).size());
  }

  @Test
  public void testExtraTasksAreNotAssignedDuringReassignment() {
    String[] instances = new String[]{"instance1"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = createStickyMulticastStrategyObject(Optional.of(4));
    Map<String, Set<DatastreamTask>> assignment1 =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    Map<String, Set<DatastreamTask>> assignment2 =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Set<DatastreamTask> assignmentTasks1 = assignment1.get(instance);
      Set<DatastreamTask> assignmentTasks2 = assignment2.get(instance);
      Assert.assertEquals(assignmentTasks1.size(), assignmentTasks2.size());
      Assert.assertEquals(assignmentTasks1.size(), 4 * 5);
      assignmentTasks1.addAll(assignmentTasks2);
    }

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment1);
    for (String instance : instances) {
      Set<DatastreamTask> newassignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(newassignmentTasks.size(), 4 * 5);
    }
  }

  private static String assignmentToString(Map<String, Set<DatastreamTask>> assignment) {
    StringBuilder builder = new StringBuilder();
    assignment.keySet().stream().sorted().forEach(instance -> {
      builder.append("Instance: ").append(instance).append(" tasks: {");
      assignment.get(instance)
          .stream()
          .map(DatastreamTask::getTaskPrefix)
          .sorted()
          .forEach(taskPrefix -> builder.append(" ").append(taskPrefix).append(','));
      if (builder.charAt(builder.length() - 1) == ',') {
        builder.setLength(builder.length() - 1); // Delete the last comma
      }
      builder.append("} ");
    });
    return builder.toString();
  }


  private static int editDiff(Map<String, Set<DatastreamTask>> assignment1,
      Map<String, Set<DatastreamTask>> assignment2) {
    Map<SimplifiedDatastreamTask, Integer> allTasks1 = new HashMap<>();
    Map<SimplifiedDatastreamTask, Integer> allTasks2 = new HashMap<>();

    for (SimplifiedDatastreamTask task : getFullTaskList(assignment1)) {
      allTasks1.merge(task, 1, Integer::sum);
    }

    for (SimplifiedDatastreamTask task : getFullTaskList(assignment2)) {
      allTasks2.merge(task, 1, Integer::sum);
    }

    Map<SimplifiedDatastreamTask, Integer> diff1 = new HashMap<>(allTasks1);
    allTasks2.forEach((key, value) -> {
      if (diff1.containsKey(key)) {
        int difference = diff1.get(key) - value;
        diff1.put(key, Math.max(difference, 0));
      }
    });

    Map<SimplifiedDatastreamTask, Integer> diff2 = new HashMap<>(allTasks2);
    allTasks1.forEach((key, value) -> {
      if (diff2.containsKey(key)) {
        int difference = diff2.get(key) - value;
        diff2.put(key, Math.max(difference, 0));
      }
    });

    return diff1.values().stream().mapToInt(i -> i).sum() + diff2.values().stream().mapToInt(i -> i).sum();
  }

  private static List<SimplifiedDatastreamTask> getFullTaskList(Map<String, Set<DatastreamTask>> assignment) {
    List<SimplifiedDatastreamTask> allTasks = new ArrayList<>();
    for (String instance : assignment.keySet()) {
      for (DatastreamTask task : assignment.get(instance)) {
        allTasks.add(new SimplifiedDatastreamTask(instance, task.getTaskPrefix(), task.getId()));
      }
    }
    return allTasks;
  }

  private static class SimplifiedDatastreamTask {
    String instance;
    String prefix;
    String id;

    SimplifiedDatastreamTask(String instance, String prefix, String id) {
      this.instance = instance;
      this.prefix = prefix;
      this.id = id;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SimplifiedDatastreamTask that = (SimplifiedDatastreamTask) o;
      return Objects.equals(instance, that.instance) && Objects.equals(prefix, that.prefix);
    }

    @Override
    public int hashCode() {
      return Objects.hash(instance, prefix);
    }
  }
}
