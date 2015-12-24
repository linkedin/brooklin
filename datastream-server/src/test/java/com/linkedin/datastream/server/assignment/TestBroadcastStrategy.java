package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


public class TestBroadcastStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(BroadcastStrategy.class.getName());

  @Test
  public void testBroadcastStrategy_createsAssignmentAcrossAllInstances() {
    String[] instances = new String[] { "instance1", "instance2", "instance3" };
    List<Datastream> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), datastreams.size());
    }
  }

  private List<Datastream> generateDatastreams(String namePrefix, int numberOfDatastreams) {
    List<Datastream> datastreams = new ArrayList<>();
    String type = DummyConnector.CONNECTOR_TYPE;
    for (int index = 0; index < numberOfDatastreams; index++) {
      Datastream ds = DatastreamTestUtils.createDatastream(type, namePrefix + index, "DummySource");
      ds.getMetadata().put("owner", "person_" + index);
      datastreams.add(ds);
    }
    return datastreams;
  }

  @Test
  public void testBroadcastStrategy_doesntCreateNewTasks_WhenCalledSecondTime() {
    String[] instances = new String[] { "instance1", "instance2", "instance3" };
    List<Datastream> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy();
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
  public void testBroadcastStrategy_createsNewTasksOnlyForNewDatastream_WhenDatastreamIsCreated() {
    List<String> instances = Arrays.asList(new String[] { "instance1", "instance2", "instance3" });
    List<Datastream> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy();
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());

    List<Datastream> newDatastreams = new ArrayList<>(datastreams);
    Datastream newDatastream = generateDatastreams("newds", 1).get(0);
    newDatastreams.add(newDatastream);

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(newDatastreams, instances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size() + 1, newAssignmentTasks.size());
      Assert.assertTrue(oldAssignmentTasks.stream().allMatch(
          x -> x.getDatastreams().contains(newDatastream.getName()) || newAssignmentTasks.contains(x)));
    }
  }

  @Test
  public void testBroadcastStrategy_createsNewTasksOnlyForNewInstance_WhenInstanceIsAdded() {
    List<String> instances = Arrays.asList(new String[] { "instance1", "instance2", "instance3" });
    String instance4 = "instance4";
    List<Datastream> datastreams = generateDatastreams("ds", 5);
    BroadcastStrategy strategy = new BroadcastStrategy();
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(datastreams, instances, new HashMap<>());
    List<String> newInstances = new ArrayList<>(instances);
    newInstances.add(instance4);
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, newInstances, assignment);

    // Ensure that the datastream tasks for the existing instances didn't change.
    for (String instance : instances) {
      Set<DatastreamTask> oldAssignmentTasks = assignment.get(instance);
      Set<DatastreamTask> newAssignmentTasks = newAssignment.get(instance);
      Assert.assertEquals(oldAssignmentTasks.size(), newAssignmentTasks.size());
      Assert.assertTrue(oldAssignmentTasks.stream().allMatch(x -> newAssignmentTasks.contains(x)));
    }

    Assert.assertEquals(newAssignment.get(instance4).size(), datastreams.size());
  }
}
