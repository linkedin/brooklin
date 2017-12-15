package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
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
import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.DEFAULT_MAX_TASKS;


public class TestStickyMulticastStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(TestBroadcastStrategy.class.getName());

  @Test
  public void testBroadcastStrategyCreatesAssignmentAcrossAllInstances() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(DEFAULT_MAX_TASKS);
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), datastreams.size());
    }
  }

  @Test
  public void testBroadcastStrategyMaxTasks() {
    int numDatastreams = 10;
    int numInstances = 20;
    int maxTasks = 7;
    int expectedTotalTasks = numDatastreams * maxTasks;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    doTestMaxTasks(new StickyMulticastStrategy(maxTasks), numInstances, expectedTotalTasks, datastreams);
  }

  @Test
  public void testBroadcastStrategyMaxTasks2() {
    int numDatastreams = 25;
    int numInstances = 32;
    int maxTasks = 6;
    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    datastreams.get(0).getDatastreams().get(0).getMetadata().put(CFG_MAX_TASKS, "18");

    int expectedTotalTasks = numDatastreams * maxTasks + (18 - maxTasks);
    doTestMaxTasks(new StickyMulticastStrategy(maxTasks), numInstances, expectedTotalTasks, datastreams);
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
  public void testBroadcastStrategyEditDiff() {
    int numDatastreams = 11;
    int numInstances = 20;
    int maxTasks = 7;
    int expectedTotalTasks = numDatastreams * maxTasks;

    List<DatastreamGroup> datastreams = generateDatastreams("ds", numDatastreams);
    List<String> instances = IntStream.range(0, numInstances).mapToObj(x -> "instance" + x).collect(Collectors.toList());

    Map<String, Set<DatastreamTask>> assignment1 =
        new StickyMulticastStrategy(maxTasks).assign(datastreams, instances, new HashMap<>());

    Assert.assertEquals(getFullTaskList(assignment1).size(), expectedTotalTasks);
    Assert.assertEquals(editDiff(assignment1, new HashMap<>()), expectedTotalTasks);

    // Test instance goes down: "instance
    Random rnd = new Random();
    String instanceDown = instances.remove(rnd.nextInt(instances.size()));
    int numTasksInInstanceDown = assignment1.get(instanceDown).size();

    Map<String, Set<DatastreamTask>> assignment2 =
        new StickyMulticastStrategy(maxTasks).assign(datastreams, instances, assignment1);
    Assert.assertTrue(editDiff(assignment1, assignment2) / (numTasksInInstanceDown + 1) <= 2);

    // Test instance coming up
    instances.add(instanceDown);
    Map<String, Set<DatastreamTask>> assignment3 =
        new StickyMulticastStrategy(maxTasks).assign(datastreams, instances, assignment2);
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
  public void testBroadcastStrategyDoesntCreateNewTasksWhenCalledSecondTime() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(DEFAULT_MAX_TASKS);
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
  }

  @Test
  public void testBroadcastStrategyRemovesDatastreamTasksWhenDatastreamIsDeleted() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(DEFAULT_MAX_TASKS);
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());

    datastreams.remove(0);

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() - 1, newAssignmentTasks.size());
    }
  }

  @Test
  public void testBroadcastStrategyCreatesNewTasksOnlyForNewDatastreamWhenDatastreamIsCreated() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(DEFAULT_MAX_TASKS);
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
  }

  @Test
  public void testBroadcastStrategyCreatesNewTasksOnlyForNewInstanceWhenInstanceIsAdded() {
    List<String> instances = Arrays.asList("instance1", "instance2", "instance3");
    String instance4 = "instance4";
    List<DatastreamGroup> datastreams = generateDatastreams("ds", 5);
    StickyMulticastStrategy strategy = new StickyMulticastStrategy(DEFAULT_MAX_TASKS);
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());
    List<String> newInstances = new ArrayList<>(instances);
    newInstances.add(instance4);
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, newInstances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size(), newAssignmentTasks.size());
      Assert.assertTrue(oldAssignmentTasks.stream().allMatch(newAssignmentTasks::contains));
    }

    Assert.assertEquals(newAssignment.get(instance4).size(), datastreams.size());
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
    Set<String> allTasks1 = getFullTaskList(assignment1);
    Set<String> allTasks2 = getFullTaskList(assignment2);

    Set<String> diff1 = new HashSet<>(allTasks1);
    diff1.removeAll(allTasks2);

    Set<String> diff2 = new HashSet<>(allTasks2);
    diff2.removeAll(allTasks1);

    return diff1.size() + diff2.size();
  }

  private static Set<String> getFullTaskList(Map<String, Set<DatastreamTask>> assignment) {
    Set<String> allTasks1 = new HashSet<>();
    for (String instance : assignment.keySet()) {
      for (DatastreamTask task : assignment.get(instance)) {
        allTasks1.add(instance + " : " + task.getTaskPrefix());
      }
    }
    return allTasks1;
  }
}
