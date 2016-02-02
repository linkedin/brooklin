package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

public class TestLoadbalancingStrategy {

  @Test
  public void testLoadbalancingStrategyDistributesTasksAcrossInstancesEqually() {
    String[] instances = new String[] { "instance1", "instance2", "instance3" };
    List<Datastream> datastreams =  Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
    }
  }


  @Test
  public void testLoadbalancingStrategyCreatesTasksOnlyForPartitionsInDestination() {
    String[] instances = new String[] { "instance1", "instance2", "instance3" };
    List<Datastream> datastreams =  Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(2));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    List<String> sortedInstances = Arrays.asList(instances);
    Collections.sort(sortedInstances);

    Assert.assertEquals(assignment.get(sortedInstances.get(0)).size(), 1);
    Assert.assertEquals(assignment.get(sortedInstances.get(1)).size(), 1);
    Assert.assertEquals(assignment.get(sortedInstances.get(2)).size(), 0);
  }

  @Test
  public void testLoadbalancingStrategyRedistributesTasksWhenNodeGoesDown() {
    String[] instances = new String[] { "instance1", "instance2", "instance3" };
    List<Datastream> datastreams =  Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
    }

    String[] newInstances = new String[] { "instance1", "instance2" };
    Map<String, Set<DatastreamTask>> newAssignment =
        strategy.assign(datastreams, Arrays.asList(newInstances), assignment);

    Assert.assertEquals(newAssignment.get(newInstances[0]).size(), 3);
    Assert.assertEquals(newAssignment.get(newInstances[1]).size(), 3);
  }

  @Test
  public void testLoadbalancingStrategyRedistributesTasksWhenNodeIsAdded() {
    String[] instances = new String[] { "instance1", "instance2" };
    List<Datastream> datastreams =  Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
    }

    String[] newInstances = new String[] { "instance1", "instance2", "instance3", "instance4" };
    Map<String, Set<DatastreamTask>> newAssignment =
        strategy.assign(datastreams, Arrays.asList(newInstances), assignment);

    for (String instance : newInstances) {
      Assert.assertEquals(newAssignment.get(instance).size(), 1);
    }
  }

  @Test
  public void testLoadbalancingStrategyCreatesNewDatastreamTasksWhenNewDatastreamIsAdded() {
    String[] instances = new String[] { "instance1", "instance2", "instance3" };
    List<Datastream> datastreams =  Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
    }

    datastreams = new ArrayList<>(datastreams);
    datastreams.add(DatastreamTestUtils.createDatastream(DummyConnector.CONNECTOR_TYPE, "ds2", "source"));
    datastreams.forEach(x -> {
      x.getSource().setPartitions(12);
    });

    Map<String, Set<DatastreamTask>> newAssignment =
        strategy.assign(datastreams, Arrays.asList(instances), assignment);

    for (String instance : instances) {
      Assert.assertEquals(newAssignment.get(instance).size(), 4);
    }
  }

  @Test
  public void testLoadbalancingStrategyRemovesTasksWhenDatastreamIsDeleted() {
    String[] instances = new String[] { "instance1", "instance2", "instance3" };
    List<Datastream> datastreams =  Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1", "ds2"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 4);
    }

    datastreams = new ArrayList<>(datastreams);
    datastreams.remove(1);

    Map<String, Set<DatastreamTask>> newAssignment =
        strategy.assign(datastreams, Arrays.asList(instances), assignment);

    for (String instance : instances) {
      Assert.assertEquals(newAssignment.get(instance).size(), 2);
    }
  }
}
