package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
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
    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(12);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    List<DatastreamGroup> datastreams = Collections.singletonList(new DatastreamGroup(Collections.singletonList(ds1)));
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
    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(12);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    List<DatastreamGroup> datastreams = Collections.singletonList(new DatastreamGroup(Collections.singletonList(ds1)));
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
    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(2);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    List<DatastreamGroup> datastreams = Collections.singletonList(new DatastreamGroup(Collections.singletonList(ds1)));
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
    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(12);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    List<DatastreamGroup> datastreams = Collections.singletonList(new DatastreamGroup(Collections.singletonList(ds1)));
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
  public void testIncompleteCurrentAssignment() {
    String[] instances = new String[]{"instance1", "instance2", "instance3"};
    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(12);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    List<DatastreamGroup> datastreams = Collections.singletonList(new DatastreamGroup(Collections.singletonList(ds1)));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    ZkAdapter adapter = createMockAdapter();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
      assignment.get(instance).forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));
    }


    // Simulate some data corruption.
    assignment.get(instances[0]).clear();
    Map<String, Set<DatastreamTask>> newAssignment =
        strategy.assign(datastreams, Arrays.asList(instances), assignment);

    // For the moment the assumption is that the system will not create new tasks
    // and just re-distribute the existing tasks over all the instances.
    Assert.assertEquals(newAssignment.get(instances[0]).size(), 2);
    Assert.assertEquals(newAssignment.get(instances[1]).size(), 1);
    Assert.assertEquals(newAssignment.get(instances[2]).size(), 1);
  }

  @Test
  public void testLoadbalancingStrategyRedistributesTasksWhenNodeIsAdded() {
    String[] instances = new String[]{"instance1", "instance2"};
    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(12);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    List<DatastreamGroup> datastreams = Collections.singletonList(new DatastreamGroup(Collections.singletonList(ds1)));
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
    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(12);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    List<DatastreamGroup> datastreams = Collections.singletonList(new DatastreamGroup(Collections.singletonList(ds1)));
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    ZkAdapter adapter = createMockAdapter();
    Map<String, Set<DatastreamTask>> assignment =
        strategy.assign(datastreams, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 2);
      assignment.get(instance).forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));
    }

    datastreams = new ArrayList<>(datastreams);
    Datastream ds2 = DatastreamTestUtils.createDatastream(DummyConnector.CONNECTOR_TYPE, "ds2", "source");
    ds2.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds2));
    ds2.getSource().setPartitions(12);
    datastreams.add(new DatastreamGroup(Collections.singletonList(ds2)));

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

    datastreams.forEach(
        x -> x.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(x)));
    List<DatastreamGroup> dgs =
        datastreams.stream().map(x -> new DatastreamGroup(Collections.singletonList(x))).collect(Collectors.toList());
    LoadbalancingStrategy strategy = new LoadbalancingStrategy();
    ZkAdapter adapter = createMockAdapter();
    Map<String, Set<DatastreamTask>> assignment = strategy.assign(dgs, Arrays.asList(instances), new HashMap<>());

    for (String instance : instances) {
      Assert.assertEquals(assignment.get(instance).size(), 4);
      assignment.get(instance).forEach(t -> ((DatastreamTaskImpl) t).setZkAdapter(adapter));
    }

    dgs = new ArrayList<>(dgs);
    dgs.remove(1);

    Map<String, Set<DatastreamTask>> newAssignment = strategy.assign(dgs, Arrays.asList(instances), assignment);

    for (String instance : instances) {
      Assert.assertEquals(newAssignment.get(instance).size(), 2);
    }
  }
}
