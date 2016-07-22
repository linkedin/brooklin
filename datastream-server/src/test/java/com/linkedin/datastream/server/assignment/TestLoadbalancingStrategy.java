package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.DatastreamTaskStatus;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


public class TestLoadbalancingStrategy {
  private ZkAdapter createMockAdapter() {
    ZkAdapter adapter = mock(ZkAdapter.class);
    Map<DatastreamTask, Map<String, String>> stateMap = new HashMap<>();
    doAnswer((invocation) -> {
      DatastreamTask task = (DatastreamTask) invocation.getArguments()[0];
      String key = (String) invocation.getArguments()[1];
      String val = (String) invocation.getArguments()[2];
      if (!stateMap.containsKey(task)) {
        stateMap.put(task, new HashMap<>());
      }
      stateMap.get(task).put(key, val);
      return null;
    }).when(adapter).setDatastreamTaskStateForKey(anyObject(), anyString(), anyString());
    doAnswer((invocation) -> {
      DatastreamTask task = (DatastreamTask) invocation.getArguments()[0];
      String key = (String) invocation.getArguments()[1];
      if (!stateMap.containsKey(task)) {
        return null;
      }
      return stateMap.get(task).get(key);
    }).when(adapter).getDatastreamTaskStateForKey(anyObject(), anyString());
    return adapter;
  }

  @Test
  public void testLoadbalancingStrategyDistributesTasksAcrossInstancesEqually() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
    }
  }

  @Test
  public void testLoadbalancingStrategyCreatesMinTasks() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    Properties strategyProps = new Properties();
    strategyProps.put(LoadbalancingStrategy.CFG_MIN_TASKS, "12");
    LoadbalancingStrategy strategy = new LoadbalancingStrategy(strategyProps);
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());
    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 4);
    }
  }

  @Test
  public void testLoadbalancingStrategyCreatesTasksOnlyForPartitionsInDestination() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
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
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    ZkAdapter adapter = createMockAdapter();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
      assignment.get(instance).forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));
    }

    String[] newInstances = new String[]{"instance1", "instance2"};
    Map<String, Set<DatastreamTask>> newAssignment =
        strategy.assign(datastreams, Arrays.asList(newInstances), assignment);

    Assert.assertEquals(newAssignment.get(newInstances[0]).size(), 3);
    Assert.assertEquals(newAssignment.get(newInstances[1]).size(), 3);
  }

  @Test
  public void testLoadbalancingStrategyRedistributesTasksWhenNodeIsAdded() {
    String[] instances = new String[]{"instance1", "instance2"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    ZkAdapter adapter = createMockAdapter();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
      assignment.get(instance).forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));
    }

    String[] newInstances = new String[]{"instance1", "instance2", "instance3", "instance4"};
    Map<String, Set<DatastreamTask>> newAssignment =
        strategy.assign(datastreams, Arrays.asList(newInstances), assignment);

    for (String instance : newInstances) {
      Assert.assertEquals(newAssignment.get(instance).size(), 1);
    }
  }

  @Test
  public void testLoadbalancingStrategyCreatesNewDatastreamTasksWhenNewDatastreamIsAdded() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    ZkAdapter adapter = createMockAdapter();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
      assignment.get(instance).forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));
    }

    datastreams = new ArrayList<>(datastreams);
    datastreams.add(DatastreamTestUtils.createDatastream(DummyConnector.CONNECTOR_TYPE, "ds2", "source"));
    datastreams.forEach(x -> {
      x.getSource().setPartitions(12);
    });

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);

    for (String instance : instances) {
      Assert.assertEquals(newAssignment.get(instance).size(), 4);
    }
  }

  @Test
  public void testLoadbalancingStrategyRemovesTasksWhenDatastreamIsDeleted() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1", "ds2"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    ZkAdapter adapter = createMockAdapter();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 4);
      assignment.get(instance).forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));
    }

    datastreams = new ArrayList<>(datastreams);
    datastreams.remove(1);

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);

    for (String instance : instances) {
      Assert.assertEquals(newAssignment.get(instance).size(), 2);
    }
  }

  @Test
  public void testFilteringCompleteTasks() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    List<Datastream> datastreams =
        Arrays.asList(DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1", "ds2"));
    datastreams.forEach(x -> x.getSource().setPartitions(12));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    List<DatastreamTask> tasks = new ArrayList<>();

    assignment.values().forEach(tasks::addAll);

    ZkAdapter adapter = createMockAdapter();
    tasks.stream().forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));

    // At least two tasks should be created
    Assert.assertTrue(tasks.size() >= 2);

    int numPending = tasks.size();
    for (int i = 0; i < tasks.size(); i += 2, numPending--) {
      tasks.get(i).setStatus(DatastreamTaskStatus.complete());
    }

    assignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);
    Assert.assertEquals(assignment.values().stream().mapToInt(Set::size).sum(), numPending);

    // Complete all
    tasks.forEach(t -> t.setStatus(DatastreamTaskStatus.complete()));
    assignment = strategy.assign(datastreams, Arrays.asList(instances), assignment);
    Assert.assertEquals(assignment.values().stream().mapToInt(Set::size).sum(), 0);
  }
}
