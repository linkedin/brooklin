/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.PartitionThroughputInfo;
import com.linkedin.datastream.server.providers.PartitionThroughputProvider;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;


/**
 * Tests for {@link LoadBasedPartitionAssignmentStrategy}
 */
@Test
public class TestLoadBasedPartitionAssignmentStrategy {
  private ZkClient _zkClient;
  private String _clusterName;

  @BeforeMethod
  public void setup() throws IOException {
    DynamicMetricsManager.createInstance(new MetricRegistry(), "TestStickyPartitionAssignment");
    _clusterName = "testCluster";
    EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper();
    String zkConnectionString = embeddedZookeeper.getConnection();
    embeddedZookeeper.startup();
    _zkClient = new ZkClient(zkConnectionString);
  }

  @Test
  public void fallbackToBaseClassWhenElasticTaskAssignmentDisabledTest() {
    PartitionThroughputProvider mockProvider = mock(PartitionThroughputProvider.class);
    boolean enableElasticTaskAssignment = false;
    Optional<Integer> maxTasks = Optional.of(100);
    Optional<Integer> imbalanceThreshold = Optional.of(50);
    Optional<Integer> maxPartitionPerTask = Optional.of(100);
    Optional<Integer> partitionsPerTask = Optional.of(50);
    Optional<Integer> partitionFullnessFactorPct = Optional.of(80);
    Optional<Integer> taskCapacityMBps = Optional.of(5);
    Optional<Integer> taskCapacityUtilizationPct = Optional.of(90);
    Optional<Integer> throughputInfoFetchTimeoutMs = Optional.of(1000);
    Optional<Integer> throughputInfoFetchRetryPeriodMs = Optional.of(200);
    Optional<ZkClient> zkClient = Optional.empty();

    LoadBasedPartitionAssignmentStrategy strategy = new LoadBasedPartitionAssignmentStrategy(mockProvider,
        maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, taskCapacityMBps, taskCapacityUtilizationPct, throughputInfoFetchTimeoutMs,
        throughputInfoFetchRetryPeriodMs, zkClient, _clusterName);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));

    DatastreamGroup datastreamGroup = new DatastreamGroup(Collections.singletonList(ds1));
    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(datastreamGroup,
        Collections.singletonList("P1"));
    strategy.assignPartitions(currentAssignment, metadata);
    Assert.assertFalse(strategy.isElasticTaskAssignmentEnabled(datastreamGroup));

    // Verify that partition throughput provider is not used when elastic task assignment is disabled
    Mockito.verify(mockProvider, times(0)).getThroughputInfo();
    Mockito.verify(mockProvider, times(0)).getThroughputInfo(any(DatastreamGroup.class));
    Mockito.verify(mockProvider, times(0)).getThroughputInfo(any(String.class));
  }

  @Test
  public void fallbackToBaseClassWhenThroughputFetchFailsTest() {
    PartitionThroughputProvider mockProvider = mock(PartitionThroughputProvider.class);
    Mockito.when(mockProvider.getThroughputInfo(any(DatastreamGroup.class))).thenThrow(new RuntimeException());
    boolean enableElasticTaskAssignment = true;
    Optional<Integer> maxTasks = Optional.of(100);
    Optional<Integer> imbalanceThreshold = Optional.of(50);
    Optional<Integer> maxPartitionPerTask = Optional.of(100);
    Optional<Integer> partitionsPerTask = Optional.of(50);
    Optional<Integer> partitionFullnessFactorPct = Optional.of(80);
    Optional<Integer> taskCapacityMBps = Optional.of(5);
    Optional<Integer> taskCapacityUtilizationPct = Optional.of(90);
    Optional<Integer> throughputInfoFetchTimeoutMs = Optional.of(1000);
    Optional<Integer> throughputInfoFetchRetryPeriodMs = Optional.of(200);
    Optional<ZkClient> zkClient = Optional.of(_zkClient);

    LoadBasedPartitionAssignmentStrategy strategy = Mockito.spy(new LoadBasedPartitionAssignmentStrategy(mockProvider,
        maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, taskCapacityMBps, taskCapacityUtilizationPct, throughputInfoFetchTimeoutMs,
        throughputInfoFetchRetryPeriodMs, zkClient, _clusterName));

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    ds1.getMetadata().put(StickyPartitionAssignmentStrategy.CFG_MIN_TASKS, String.valueOf(10));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));

    DatastreamGroup datastreamGroup = new DatastreamGroup(Collections.singletonList(ds1));
    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(datastreamGroup,
        Collections.singletonList("P1"));
    Assert.assertTrue(strategy.isElasticTaskAssignmentEnabled(datastreamGroup));
    Map<String, Set<DatastreamTask>> newAssignment = strategy.assignPartitions(currentAssignment, metadata);

    Mockito.verify(mockProvider, atLeastOnce()).getThroughputInfo(any(DatastreamGroup.class));
    Mockito.verify(strategy, never()).doAssignment(anyObject(), anyObject(), anyObject(), anyObject());
    Assert.assertNotNull(newAssignment);
  }

  @Test
  public void doesntFetchPartitionInfoOnIncrementalAssignmentTest() {
    PartitionThroughputProvider mockProvider = mock(PartitionThroughputProvider.class);
    boolean enableElasticTaskAssignment = true;
    Optional<Integer> maxTasks = Optional.of(100);
    Optional<Integer> imbalanceThreshold = Optional.of(50);
    Optional<Integer> maxPartitionPerTask = Optional.of(100);
    Optional<Integer> partitionsPerTask = Optional.of(50);
    Optional<Integer> partitionFullnessFactorPct = Optional.of(80);
    Optional<Integer> taskCapacityMBps = Optional.of(5);
    Optional<Integer> taskCapacityUtilizationPct = Optional.of(90);
    Optional<Integer> throughputInfoFetchTimeoutMs = Optional.of(1000);
    Optional<Integer> throughputInfoFetchRetryPeriodMs = Optional.of(200);
    Optional<ZkClient> zkClient = Optional.of(_zkClient);

    LoadBasedPartitionAssignmentStrategy strategy = new LoadBasedPartitionAssignmentStrategy(mockProvider,
        maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, taskCapacityMBps, taskCapacityUtilizationPct, throughputInfoFetchTimeoutMs,
        throughputInfoFetchRetryPeriodMs, zkClient, _clusterName);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getSource().setPartitions(0);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    DatastreamTask task = createTaskForDatastream(ds1, Collections.singletonList("P1"));
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(task)));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds1)), Collections.singletonList("P2"));
    strategy.assignPartitions(currentAssignment, metadata);

    // Verify that partition throughput provider is not used when the current assignment is not empty
    Mockito.verify(mockProvider, times(0)).getThroughputInfo();
    Mockito.verify(mockProvider, times(0)).getThroughputInfo(any(DatastreamGroup.class));
    Mockito.verify(mockProvider, times(0)).getThroughputInfo(any(String.class));
  }

  @Test
  public void updatesNumTasksAndThrowsExceptionWhenNoSufficientTasksTest() {
    PartitionThroughputProvider mockProvider = mock(PartitionThroughputProvider.class);
    Map<String, PartitionThroughputInfo> partitionThroughputMap = new HashMap<>();
    partitionThroughputMap.put("P1", new PartitionThroughputInfo(100000, 0, "P1"));
    partitionThroughputMap.put("P2", new PartitionThroughputInfo(100000, 0, "P2"));
    partitionThroughputMap.put("P3", new PartitionThroughputInfo(100000, 0, "P3"));
    ClusterThroughputInfo clusterThroughputInfo = new ClusterThroughputInfo(StringUtils.EMPTY, partitionThroughputMap);
    Mockito.when(mockProvider.getThroughputInfo(any(DatastreamGroup.class))).thenReturn(clusterThroughputInfo);
    boolean enableElasticTaskAssignment = true;
    Optional<Integer> maxTasks = Optional.of(100);
    Optional<Integer> imbalanceThreshold = Optional.of(50);
    Optional<Integer> maxPartitionPerTask = Optional.of(100);
    Optional<Integer> partitionsPerTask = Optional.of(50);
    Optional<Integer> partitionFullnessFactorPct = Optional.of(80);
    Optional<Integer> taskCapacityMBps = Optional.of(5);
    Optional<Integer> taskCapacityUtilizationPct = Optional.of(90);
    Optional<Integer> throughputInfoFetchTimeoutMs = Optional.of(1000);
    Optional<Integer> throughputInfoFetchRetryPeriodMs = Optional.of(200);
    Optional<ZkClient> zkClient = Optional.of(_zkClient);

    LoadBasedPartitionAssignmentStrategy strategy = new LoadBasedPartitionAssignmentStrategy(mockProvider,
        maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, taskCapacityMBps, taskCapacityUtilizationPct, throughputInfoFetchTimeoutMs,
        throughputInfoFetchRetryPeriodMs, zkClient, _clusterName);

    Datastream ds1 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds1")[0];
    ds1.getMetadata().put(StickyPartitionAssignmentStrategy.CFG_MIN_TASKS, String.valueOf(10));
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    ds1.getSource().setPartitions(0);
    String taskPrefix = DatastreamTaskImpl.getTaskPrefix(ds1);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, taskPrefix);
    _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, taskPrefix));
    Map<String, Set<DatastreamTask>> currentAssignment = new HashMap<>();
    currentAssignment.put("instance1", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds1))));

    DatastreamGroupPartitionsMetadata metadata = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds1)), Arrays.asList("P1", "P2"));
    Assert.expectThrows(DatastreamRuntimeException.class, () -> strategy.assignPartitions(currentAssignment, metadata));
    int numTasks = getNumTasksForDatastreamFromZK(taskPrefix);
    Assert.assertEquals(numTasks, 2);

    // make sure throughput info is fetched
    Mockito.verify(mockProvider, atLeastOnce()).getThroughputInfo(any(DatastreamGroup.class));

    // test that strategy honors maxTasks config
    Datastream ds2 = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds2")[0];
    ds2.getMetadata().put(StickyPartitionAssignmentStrategy.CFG_MIN_TASKS, String.valueOf(1));
    ds2.getMetadata().put(BroadcastStrategyFactory.CFG_MAX_TASKS, String.valueOf(2));
    ds2.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds2));
    ds2.getSource().setPartitions(0);
    String taskPrefix2 = DatastreamTaskImpl.getTaskPrefix(ds2);
    ds2.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, taskPrefix2);
    _zkClient.ensurePath(KeyBuilder.datastream(_clusterName, taskPrefix2));
    Map<String, Set<DatastreamTask>> currentAssignment2 = new HashMap<>();
    currentAssignment2.put("instance1", new HashSet<>(Collections.singletonList(createTaskForDatastream(ds2))));

    DatastreamGroupPartitionsMetadata metadata2 = new DatastreamGroupPartitionsMetadata(new DatastreamGroup(
        Collections.singletonList(ds2)), Arrays.asList("P1", "P2", "P3"));
    Assert.expectThrows(DatastreamRuntimeException.class, () -> strategy.assignPartitions(currentAssignment2, metadata2));
    int numTasks2 = getNumTasksForDatastreamFromZK(taskPrefix2);
    // updated numTasks must be no bigger than 2
    Assert.assertEquals(numTasks2, 2);
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

  private int getNumTasksForDatastreamFromZK(String taskPrefix) {
    String numTasksPath = KeyBuilder.datastreamNumTasks(_clusterName, taskPrefix);
    return Integer.parseInt(_zkClient.readData(numTasksPath));
  }
}
