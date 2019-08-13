/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.kafka.KafkaDestination;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.security.Authorizer;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.server.assignment.LoadbalancingStrategy;
import com.linkedin.datastream.server.assignment.StickyMulticastStrategy;
import com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategy;
import com.linkedin.datastream.server.dms.DatastreamResources;
import com.linkedin.datastream.server.dms.DatastreamStore;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;
import com.linkedin.datastream.server.providers.CheckpointProvider;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.BatchUpdateRequest;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.RestLiServiceException;
import com.linkedin.restli.server.UpdateResponse;

import static com.linkedin.datastream.common.DatastreamMetadataConstants.CREATION_MS;
import static com.linkedin.datastream.common.DatastreamMetadataConstants.SYSTEM_DESTINATION_PREFIX;
import static com.linkedin.datastream.common.DatastreamMetadataConstants.TTL_MS;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link Coordinator}
 */
public class TestCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(TestCoordinator.class);
  private static final long WAIT_DURATION_FOR_ZK = Duration.ofMinutes(1).toMillis();
  private static final int WAIT_TIMEOUT_MS = 60000;

  EmbeddedZookeeper _embeddedZookeeper;
  String _zkConnectionString;
  private CachedDatastreamReader _cachedDatastreamReader;

  static {
    DynamicMetricsManager.createInstance(new MetricRegistry(), "TestCoordinator");
  }

  private Coordinator createCoordinator(String zkAddr, String cluster) throws Exception {
    return createCoordinator(zkAddr, cluster, new Properties());
  }

  private Coordinator createCoordinator(String zkAddr, String cluster, Properties override) throws Exception {
    return createCoordinator(zkAddr, cluster, override, new DummyTransportProviderAdminFactory());
  }

  private Coordinator createCoordinator(String zkAddr, String cluster, Properties override,
      TransportProviderAdminFactory transportProviderAdminFactory) throws Exception {
    Properties props = new Properties();
    props.put(CoordinatorConfig.CONFIG_CLUSTER, cluster);
    props.put(CoordinatorConfig.CONFIG_ZK_ADDRESS, zkAddr);
    props.put(CoordinatorConfig.CONFIG_ZK_SESSION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_SESSION_TIMEOUT));
    props.put(CoordinatorConfig.CONFIG_ZK_CONNECTION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_CONNECTION_TIMEOUT));
    props.putAll(override);
    ZkClient client = new ZkClient(zkAddr);
    _cachedDatastreamReader = new CachedDatastreamReader(client, cluster);
    Coordinator coordinator = new Coordinator(_cachedDatastreamReader, props);
    coordinator.addTransportProvider(DummyTransportProviderAdminFactory.PROVIDER_NAME,
        transportProviderAdminFactory.createTransportProviderAdmin(DummyTransportProviderAdminFactory.PROVIDER_NAME,
            new Properties()));
    return coordinator;
  }

  @BeforeMethod
  public void setup() throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
  }

  @AfterMethod
  public void teardown() throws IOException {
    _embeddedZookeeper.shutdown();
  }

  /**
   * testConnectorStateSetAndGet makes sure that the connector can read and write state that
   * is specific to each DatastreamTask.
   */
  @Test
  public void testConnectorStateSetAndGet() throws Exception {
    String testCluster = "testConnectorStateSetAndGet";
    String testConnectorType = "testConnectorType";

    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);
    Set<String> taskNames = new HashSet<>();
    //
    // create a Connector instance, its sole purpose is to record the number of times
    // the onAssignmentChange() is called, and it will persist this value for each
    // task
    //
    Connector testConnector = new Connector() {
      @Override
      public void start(CheckpointProvider checkpointProvider) {
      }

      @Override
      public void stop() {
      }

      @Override
      public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
        // for each instance of assigned DatastreamTask, we keep a state with the key
        // "counter". Every time onAssignmentChange() is called, we increment this counter
        // by one for each assigned task.
        tasks.forEach(task -> {
          String counter = task.getState("counter");
          if (counter == null) {
            task.saveState("counter", "1");
            taskNames.add(task.getDatastreamTaskName());
          } else {
            int c = Integer.parseInt(counter);
            task.saveState("counter", Integer.toString(c + 1));
          }
        });
      }

      @Override
      public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) {
      }

      @Override
      public List<BrooklinMetricInfo> getMetricInfos() {
        return null;
      }
    };

    coordinator.addConnector(testConnectorType, testConnector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    //
    // create a new datastream so that the onAssignmentChange() can be called
    //
    String datastreamName1 = "datastream1";
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName1);

    //
    // verify that the counter value for the connector is 1 because the onAssignmentChange
    // should be called once
    //
    PollUtils.poll(() -> taskNames.size() == 1, 500, 30000);
    String name1 = (String) taskNames.toArray()[0];
    String datastream1CounterPath = KeyBuilder.datastreamTaskStateKey(testCluster, testConnectorType, name1, "counter");
    Assert.assertTrue(PollUtils.poll(zkClient::exists, 500, 30000, datastream1CounterPath));
    Assert.assertEquals(zkClient.readData(datastream1CounterPath), "1");
    //
    // add a second datastream named datastream2
    //
    String datastreamName2 = "datastream2";
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName2);
    PollUtils.poll(() -> taskNames.size() == 2, 500, 30000);
    String name2 = (String) taskNames.toArray()[1];
    String datastream2CounterPath = KeyBuilder.datastreamTaskStateKey(testCluster, testConnectorType, name2, "counter");
    Assert.assertTrue(PollUtils.poll(zkClient::exists, 500, 30000, datastream2CounterPath));

    Thread.sleep(1000 * 60);

    //
    // clean up
    //
    zkClient.close();
    coordinator.stop();
  }

  // verify that connector znodes are created as soon as Coordinator instance is started
  @Test
  public void testConnectorZkNodes() throws Exception {
    String testCluster = "testConnectorZkNodes";
    String testConnectorType = "testConnectorType";

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    String znode = KeyBuilder.connector(testCluster, testConnectorType);
    Assert.assertTrue(zkClient.exists(znode));

    zkClient.close();
    instance1.stop();
  }

  /**
   * testCoordinationWithBroadcastStrategy is a smoke test, to verify that datastreams created by DSM can be
   * assigned to live instances. The datastreams created by DSM is mocked by directly creating
   * the znodes in ZooKeeper. The steps involved:
   * <ul>
   *     <li>create a cluster with 1 live instance named instance1, start the live instance</li>
   *     <li>create the first datastream (datastream1) with broadcast strategy, and verify it is assigned to instance1</li>
   *     <li>create a second live instance named instance2 and join the cluster</li>
   *     <li>verify that instance2 is also assigned the same datastream datastream1</li>
   *     <li>create a second datastream (datastream2)</li>
   *     <li>verify that datastream2 is assigned to both instance1 and instance2</li>
   * </ul>
   */
  @Test
  public void testCoordinationWithBroadcastStrategy() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConnectorType = "testConnectorType";
    String datastreamName1 = "datastream1";

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create datastream definitions under /testAssignmentBasic/datastream/datastream1
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName1);

    //
    // verify the instance has 1 task assigned: datastream1
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName1);

    //
    // create a second live instance named instance2 and join the cluster
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance2.start();

    //
    // verify instance2 has 1 task assigned
    //
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, datastreamName1);

    //
    // create a new datastream definition for the same connector type, /testAssignmentBasic/datastream/datastream2
    //
    String datastreamName2 = "datastream2";
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName2);

    //
    // verify both instance1 and instance2 now have two datastreamtasks assigned
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream1", "datastream2");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1", "datastream2");

    // Pause the First Datastream.
    Datastream ds1 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream1");
    ds1.setStatus(DatastreamStatus.PAUSED);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds1);

    // check that datastream2 is ok
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName2);
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, datastreamName2);

    // Verify that the Tasks for Datastream1 are parked.
    String pausedPath = KeyBuilder.instanceAssignments(testCluster, Coordinator.PAUSED_INSTANCE);
    Assert.assertEquals(zkClient.getChildren(pausedPath).size(), 2);

    // Check that the task status is Paused
    String task1Name = zkClient.getChildren(pausedPath).get(0);
    String json =
        zkClient.readData(KeyBuilder.datastreamTaskStateKey(testCluster, testConnectorType, task1Name, "STATUS"), true);
    Assert.assertEquals(JsonUtils.fromJson(json, DatastreamTaskStatus.class), DatastreamTaskStatus.paused());

    // Resume the First Datastream.
    ds1 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream1");
    ds1.setStatus(DatastreamStatus.READY);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds1);

    //
    // verify both instance1 and instance2 now have two datastreamtasks assigned
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream1", "datastream2");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1", "datastream2");

    // Verify not paused Datastream.
    Assert.assertEquals(zkClient.getChildren(pausedPath).size(), 0);

    // Check that the task status is OK or null (for broadcast datastream the coordinator sometimes create new ones)
    json =
        zkClient.readData(KeyBuilder.datastreamTaskStateKey(testCluster, testConnectorType, task1Name, "STATUS"), true);
    Assert.assertTrue(
        json == null || JsonUtils.fromJson(json, DatastreamTaskStatus.class).equals(DatastreamTaskStatus.ok()));

    // Create a Third instance, that should be deduped with datastream1
    String datastreamName3 = "datastream3";
    ds1 = DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName1);
    Datastream ds3 = ds1.copy();
    ds3.setName(datastreamName3);
    ds3.setStatus(DatastreamStatus.INITIALIZING);
    ds3.getMetadata().clear();
    ds3.getMetadata().put("owner", "SecondOwner");
    ds3.getMetadata()
        .put(DatastreamMetadataConstants.TASK_PREFIX, ds1.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, ds3);

    // Wait for DS3 to be ready
    Assert.assertTrue(PollUtils.poll(() -> DatastreamStatus.READY.equals(
        DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName3).getStatus()), 200, WAIT_TIMEOUT_MS));

    // Pause Again the First Datastream
    ds1 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream1");
    ds1.setStatus(DatastreamStatus.PAUSED);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds1);

    // Check that the datastream are running.
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream1", "datastream2");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1", "datastream2");

    // Verify that No Tasks are parked (because DS1 and DS3 are in the same group, and DS3 is not paused)
    Assert.assertEquals(zkClient.getChildren(pausedPath).size(), 0);

    // Pause The third Datastream.
    ds3 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream3");
    ds3.setStatus(DatastreamStatus.PAUSED);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds3);

    // check that datastream2 is ok
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName2);
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, datastreamName2);

    // Verify that the Tasks for Datastream1 are parked. (both DS1 and DS3 are paused)
    Assert.assertEquals(zkClient.getChildren(pausedPath).size(), 2);

    // Create a Fourth instance, that should be deduped with datastream1 and datastream3
    String datastreamName4 = "datastream4";
    ds1 = DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName1);
    Datastream ds4 = ds1.copy();
    ds4.setName(datastreamName4);
    ds4.getMetadata().clear();
    ds4.getMetadata().put("owner", "SecondOwner");
    ds4.removeDestination();
    instance1.initializeDatastream(ds4);
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, ds4);

    // Wait for DS4 to be created paused
    Assert.assertTrue(PollUtils.poll(() -> DatastreamStatus.PAUSED.equals(
        DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName4).getStatus()), 200, WAIT_TIMEOUT_MS));

    // Create a Fifth instance, that should be deduped with datastream2
    String datastreamName5 = "datastream5";
    Datastream ds2 = DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName2);
    Datastream ds5 = ds2.copy();
    ds5.setName(datastreamName5);
    ds5.getMetadata().clear();
    ds5.getMetadata().put("owner", "SecondOwner");
    ds5.removeDestination();
    instance1.initializeDatastream(ds5);
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, ds5);

    // Wait for DS5 to be created in Ready state.
    Assert.assertTrue(PollUtils.poll(() -> DatastreamStatus.READY.equals(
        DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName5).getStatus()), 200, WAIT_TIMEOUT_MS));

    // clean up
    //
    instance1.stop();
    zkClient.close();
  }

  @Test
  public void testStopAndResumeDatastream() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConnectorType = "testConnectorType";
    String datastreamName1 = "datastream1";

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName1);
    //verify the assignment
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName1);
    String instance1Path = KeyBuilder.instanceAssignments(testCluster, instance1.getInstanceName());
    Assert.assertNotEquals(zkClient.getChildren(instance1Path).size(), 0);

    // Stop the datastream.
    Datastream ds1 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream1");
    ds1.setStatus(DatastreamStatus.STOPPED);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds1);

    Assert.assertTrue(PollUtils.poll(() -> DatastreamStatus.STOPPED.equals(
        DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName1).getStatus()), 200, WAIT_TIMEOUT_MS));

    Assert.assertTrue(PollUtils.poll(() -> (zkClient.getChildren(instance1Path).size() == 0), 200, WAIT_TIMEOUT_MS));

    // Resume the datastream
    ds1.setStatus(DatastreamStatus.READY);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds1);

    Assert.assertTrue(PollUtils.poll(() -> DatastreamStatus.READY.equals(
        DatastreamTestUtils.getDatastream(zkClient, testCluster, datastreamName1).getStatus()), 200, WAIT_TIMEOUT_MS));
    Assert.assertTrue(PollUtils.poll(() -> (zkClient.getChildren(instance1Path).size() != 0), 200, WAIT_TIMEOUT_MS));

  }

    /**
     * testCoordinationWithStickyMulticastStrategy is a smoke test to verify correct rebalance behavior upon datastream
     * being paused and instance being add
     * <ul>
     *     <li>create 3 instances and 4 datastreams, verified all of them get assigned properly</li>
     *     <li>pause a data stream, verified it doesn't get rebalanced</li>
     *     <li>unpause a data stream, verified it doesn't get rebalanced</li>
     *     <li>add a instance, verify the new instance get proper assignment</li>
     * </ul>
     */
  @Test
  public void testCoordinationWithStickyMulticastStrategy() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConnectorType = "testConnectorType";
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);

    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    //Question why the multicast strategy is within one coordinator rather than shared between list of coordinators
    instance1.addConnector(testConnectorType, connector1, new StickyMulticastStrategy(Optional.of(4), Optional.of(2)), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new StickyMulticastStrategy(Optional.of(4), Optional.of(2)), false,
        new SourceBasedDeduper(), null);
    instance2.start();

    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector("connector3", testConnectorType);
    instance3.addConnector(testConnectorType, connector3, new StickyMulticastStrategy(Optional.of(4), Optional.of(2)), false,
        new SourceBasedDeduper(), null);
    instance3.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);
    List<TestHookConnector> connectors = new ArrayList<>();
    connectors.add(connector1);
    connectors.add(connector2);
    connectors.add(connector3);
    List<String> datastreamNames = ImmutableList.of("datastream1", "datastream2", "datastream3", "datastream4");

    for (String name : datastreamNames) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, name);
    }
    waitTillAssignmentIsComplete(16, WAIT_TIMEOUT_MS, connectors.toArray(new TestHookConnector[connectors.size()]));

    //Verify the assignment, each datastream should be assigned to four tasks
    Map<String, List<Connector>> assignment1 = collectDatastreamAssignment(connectors);
    assignment1.values().stream().forEach(set -> Assert.assertEquals(set.size(), 4));

    //Pause datastream 3
    Datastream ds3 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream3");
    ds3.setStatus(DatastreamStatus.PAUSED);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds3);

    waitTillAssignmentIsComplete(12, WAIT_TIMEOUT_MS, connectors.toArray(new TestHookConnector[connectors.size()]));

    //Verify the assignment for datastream1, datastream2, datastream4 are still the same, but not the case for ds3
    Map<String, List<Connector>> assignment2 = collectDatastreamAssignment(connectors);
    Assert.assertEquals(assignment1.get("datastream1"), assignment2.get("datastream1"));
    Assert.assertEquals(assignment1.get("datastream2"), assignment2.get("datastream2"));
    Assert.assertEquals(assignment1.get("datastream4"), assignment2.get("datastream4"));
    Assert.assertFalse(assignment2.containsKey("datastream3"));

    //resume the data stream
    ds3 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream3");
    ds3.setStatus(DatastreamStatus.READY);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds3);
    waitTillAssignmentIsComplete(16, WAIT_TIMEOUT_MS, connector1, connector2, connector3);

    //verify the assignment is still sticky, same as previous assignment
    Map<String, List<Connector>> assignment3 = collectDatastreamAssignment(connectors);
    Assert.assertEquals(assignment1.get("datastream1"), assignment3.get("datastream1"));
    Assert.assertEquals(assignment1.get("datastream2"), assignment3.get("datastream2"));
    Assert.assertEquals(assignment1.get("datastream4"), assignment3.get("datastream4"));
    Assert.assertEquals(assignment3.get("datastream3").size(), 4);
    waitTillAssignmentIsComplete(16, WAIT_TIMEOUT_MS, connectors.toArray(new TestHookConnector[connectors.size()]));

    //Stop the instance 1, force a leader change
    instance1.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance1);

    connectors.remove(0);
    waitTillAssignmentIsComplete(16, WAIT_TIMEOUT_MS, connectors.toArray(new TestHookConnector[connectors.size()]));

    //now add another instance, make sure it's getting rebalanced
    Coordinator instance4 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector4 = new TestHookConnector("connector4", testConnectorType);
    instance4.addConnector(testConnectorType, connector4, new StickyMulticastStrategy(Optional.of(4), Optional.of(2)), false,
        new SourceBasedDeduper(), null);
    instance4.start();

    connectors.add(connector4);
    //verify connector4 get at least 5 task assignment
    waitTillAssignmentIsComplete(5, WAIT_TIMEOUT_MS, connector4);

    instance2.stop();
    instance3.stop();
    instance4.stop();

    zkClient.close();
  }

  @Test
  public void testCoordinationWithPartitionAssignment() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConnectorType = "testConnectorType";
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);

    int initialDelays = 100;

    List<String> partitions1 = ImmutableList.of("t-0", "t-1", "t-2", "t-3", "t-4", "t-5", "t-6", "t-7", "t-8");
    List<String> partitions2 = ImmutableList.of("p-0", "p-1", "p-2", "p-3", "t-0");
    Map<String, List<String>> partitions = new HashMap<>();
    partitions.put("datastream1", partitions1);
    partitions.put("datastream2", partitions2);

    TestHookConnector connector1 = createConnectorWithPartitionListener("connector1", testConnectorType, partitions, initialDelays);

    //Question why the multicast strategy is within one coordinator rather than shared between list of coordinators
    instance1.addConnector(testConnectorType, connector1, new StickyPartitionAssignmentStrategy(Optional.of(4),
            Optional.of(2), Optional.empty()), false, new SourceBasedDeduper(), null);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = createConnectorWithPartitionListener("connector2", testConnectorType, partitions, initialDelays);
    instance2.addConnector(testConnectorType, connector2, new StickyPartitionAssignmentStrategy(Optional.of(4),
            Optional.of(2), Optional.empty()), false, new SourceBasedDeduper(), null);
    instance2.start();

    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = createConnectorWithPartitionListener("connector3", testConnectorType, partitions, initialDelays);
    instance3.addConnector(testConnectorType, connector3, new StickyPartitionAssignmentStrategy(Optional.of(4),
            Optional.of(2), Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance3.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);
    List<TestHookConnector> connectors = new ArrayList<>();
    connectors.add(connector1);
    connectors.add(connector2);
    connectors.add(connector3);

    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, "datastream1", "datastream2");

    waitTillAssignmentIsComplete(8, WAIT_TIMEOUT_MS, connectors.toArray(new TestHookConnector[connectors.size()]));

    final long interval = WAIT_TIMEOUT_MS < 100 ? WAIT_TIMEOUT_MS : 100;
    Map<String, List<String>> assignment = collectDatastreamPartitions(connectors);

    Assert.assertTrue(
        PollUtils.poll(() -> {
      //Verify all the partitions are assigned
      Map<String, List<String>> assignment2 = collectDatastreamPartitions(connectors);
      return assignment2.get("datastream1").size() == partitions1.size() && assignment2.get("datastream2").size() == partitions2.size();
    }, interval, WAIT_TIMEOUT_MS));
    instance1.stop();
    instance2.stop();
    instance3.stop();

    zkClient.close();
  }

  private TestHookConnector createConnectorWithPartitionListener(String name, String connectorType,
      Map<String, List<String>> partitions, int initialDelayMs) {
    return new TestHookConnector(name, connectorType) {

      Map<String, DatastreamGroup> _datastremGroups = new HashMap<>();
      Consumer<DatastreamGroup> _callback;
      Thread _callbackThread = null;

      @Override
      public void onPartitionChange(Consumer<DatastreamGroup> callback) {
        _callback = callback;
      }

      @Override
      public void handleDatastream(List<DatastreamGroup> datastreamGroup) {
        if (datastreamGroup.size() > 0) {
          datastreamGroup.forEach(dg -> _datastremGroups.put(dg.getName(), dg));
          _callbackThread = new Thread(() -> {
            try {
              Thread.sleep(initialDelayMs);
              for (DatastreamGroup ds : datastreamGroup) {
                _callback.accept(ds);
              }
            } catch (Exception ex) {

            }
          });
          _callbackThread.start();
        }
      }

      @Override
      public Map<String, Optional<DatastreamGroupPartitionsMetadata>> getDatastreamPartitions() {
        return _datastremGroups.values().stream().collect(Collectors.toMap(g -> g.getName(),
            g -> Optional.of(new DatastreamGroupPartitionsMetadata(g, partitions.get(g.getName())))));
      }

      @Override
      public void stop() {
        super.stop();
        if (_callbackThread != null) {
          _callbackThread.interrupt();
        }
      }
    };
  }


  private Map<String, List<Connector>> collectDatastreamAssignment(List<TestHookConnector> connectors) {
    Map<String, List<Connector>> datastreamMap = new HashMap<>();
    for (TestHookConnector testHookConnector : connectors) {
      testHookConnector.getTasks().stream().forEach(task -> {
        String datastream = task.getDatastreams().get(0).getName();
        if (!datastreamMap.containsKey(datastream)) {
          datastreamMap.put(datastream, new ArrayList<>());
        }
        datastreamMap.get(datastream).add(testHookConnector);
      });
    }
    return datastreamMap;
  }

  private Map<String, List<String>> collectDatastreamPartitions(List<TestHookConnector> connectors) {
    Map<String, List<String>> datastreamMap = new HashMap<>();
    for (TestHookConnector testHookConnector : connectors) {
      testHookConnector.getTasks().stream().forEach(task -> {
        String datastream = task.getDatastreams().get(0).getName();
        datastreamMap.putIfAbsent(datastream, new ArrayList<>());
        LOG.info("{}", task);
        datastreamMap.get(datastream).addAll(task.getPartitionsV2());
      });
    }
    return datastreamMap;
  }


  /**
   * Test Datastream create with BYOT where destination is in use by another datastream
   */
  @Test
  public void testBYOTDatastreamWithUsedDestination() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConnectorType = "testConnectorType";

    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    coordinator.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    Datastream ds1 =
        DatastreamTestUtils.createDatastream(testConnectorType, "testDatastream1", "testSource1", "testDestination1",
            32);

    DatastreamStore store = new ZookeeperBackedDatastreamStore(_cachedDatastreamReader, zkClient, testCluster);
    DatastreamResources resource = new DatastreamResources(store, coordinator);
    resource.create(ds1);

    Datastream ds2 =
        DatastreamTestUtils.createDatastream(testConnectorType, "testDatastream2", "testSource2", "testDestination1",
            32);

    try {
      resource.create(ds2);
      Assert.fail("DatastreamValidationException expected on creation of testDatastream2 with a pre-used destination");
    } catch (RestLiServiceException e) {
    }

    ds2.getDestination().setConnectionString("testDestination2"); // Should succeed with a different destination
    resource.create(ds2);
  }

  private void assertConnectorReceiveDatastreamUpdate(TestHookConnector connector, Datastream updatedDatastream)
      throws Exception {
    assertConnectorAssignment(connector, WAIT_TIMEOUT_MS, updatedDatastream.getName());
    Assert.assertTrue(
        PollUtils.poll(() -> connector.getTasks().get(0).getDatastreams().get(0).equals(updatedDatastream), 1000,
            WAIT_TIMEOUT_MS));
  }

  /**
   * Test that coordinator does not delete topics when datastream using BYOT is deleted.
   * @throws Exception
   */
  @Test
  public void testDatastreamWithBYOT() throws Exception {
    String testCluster = "testDatastreamWithBYOT";
    String testConnectorType = "testConnector";

    DummyTransportProviderAdminFactory transportProviderAdminFactory = new DummyTransportProviderAdminFactory();
    Coordinator coordinator =
        createCoordinator(_zkConnectionString, testCluster, new Properties(), transportProviderAdminFactory);
    TestHookConnector connector1 = new TestHookConnector("testDatastreamWithBYOTConnector", testConnectorType);
    coordinator.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    String datastreamName = "testBYOTDatastream";
    Datastream ds =
        DatastreamTestUtils.createDatastream(testConnectorType, datastreamName, "testDatastreamWithBYOTSource",
            "testDatastreamWithBYOTDestination", 10);

    DatastreamStore store = new ZookeeperBackedDatastreamStore(_cachedDatastreamReader, zkClient, testCluster);
    DatastreamResources resource = new DatastreamResources(store, coordinator);
    resource.create(ds);

    ds = resource.get(datastreamName);
    Assert.assertTrue(DatastreamUtils.isUserManagedDestination(ds),
        "createDatastream with pre-populated destination string should have set metadata "
            + DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY);
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName);

    resource.delete(datastreamName);
    String path = KeyBuilder.datastream(testCluster, datastreamName);
    Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(path), 200, WAIT_TIMEOUT_MS));
    Assert.assertEquals(transportProviderAdminFactory._dropDestinationCount, 0,
        "Delete destination count should have been 0, since Datastream uses BYOT");
  }

  /**
   * Test datastream creation with Connector-managed destination; coordinator should not create or delete topics.
   */
  @Test
  public void testDatastreamWithConnectorManagedDestination() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConnectorType = "testConnectorType";

    DummyTransportProviderAdminFactory transportProviderAdminFactory = new DummyTransportProviderAdminFactory();
    Coordinator coordinator =
        createCoordinator(_zkConnectionString, testCluster, new Properties(), transportProviderAdminFactory);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    coordinator.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    String datastreamName = "testDatastream1";
    Datastream ds =
        DatastreamTestUtils.createDatastreamWithoutDestination(testConnectorType, datastreamName, "testSource1");
    ds.getMetadata().put(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());

    DatastreamStore store = new ZookeeperBackedDatastreamStore(_cachedDatastreamReader, zkClient, testCluster);
    DatastreamResources resource = new DatastreamResources(store, coordinator);
    resource.create(ds);

    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName);
    Assert.assertEquals(transportProviderAdminFactory._createDestinationCount, 0,
        "Create destination count should have been 0, since Datastream has connector-managed destination");

    resource.delete(datastreamName);
    String path = KeyBuilder.datastream(testCluster, datastreamName);
    Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(path), 200, WAIT_TIMEOUT_MS));
    Assert.assertEquals(transportProviderAdminFactory._dropDestinationCount, 0,
        "Delete destination count should have been 0, since Datastream has connector-managed destination");
  }

  /**
   * Test datastream creation and deletion with regular destination; coordinator should create and delete topics
   * accordingly.
   */
  @Test
  public void testDatastreamWithoutConnectorManagedDestination() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConnectorType = "testConnectorType";

    DummyTransportProviderAdminFactory transportProviderAdminFactory = new DummyTransportProviderAdminFactory();
    Coordinator coordinator =
        createCoordinator(_zkConnectionString, testCluster, new Properties(), transportProviderAdminFactory);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    coordinator.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    String datastreamName = "testDatastream1";
    Datastream ds =
        DatastreamTestUtils.createDatastreamWithoutDestination(testConnectorType, datastreamName, "testSource1");

    DatastreamStore store = new ZookeeperBackedDatastreamStore(_cachedDatastreamReader, zkClient, testCluster);
    DatastreamResources resource = new DatastreamResources(store, coordinator);
    resource.create(ds);

    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName);
    Assert.assertEquals(transportProviderAdminFactory._createDestinationCount, 1,
        "Create destination count should have been 1, since Datastream does not have connector-managed destination");

    resource.delete(datastreamName);
    String path = KeyBuilder.datastream(testCluster, datastreamName);
    Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(path), 200, WAIT_TIMEOUT_MS));
    Assert.assertTrue(
        PollUtils.poll(() -> transportProviderAdminFactory._dropDestinationCount == 1, 1000, WAIT_TIMEOUT_MS),
        "Delete destination count should have been 1, since Datastream does not have connector-managed destination");
  }

  @Test
  public void testValidatePartitionAssignmentSupported() throws Exception {
    String testCluster = "testValidatePartitionAssignmentSupported";

    String connectorType1 = "connectorType1";
    String connectorType2 = "connectorType2";

    TestHookConnector connector1 = new TestHookConnector("connector1", connectorType1);
    TestHookConnector connector2 = new TestHookConnector("connector2", connectorType2);

    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);
    coordinator.addConnector(connectorType1, connector1, new StickyPartitionAssignmentStrategy(Optional.empty(), Optional.empty(), Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.addConnector(connectorType2, connector2, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();

    Datastream datastream1 = DatastreamTestUtils.createDatastream(connectorType1, "name1", "source1");
    Datastream datastream2 = DatastreamTestUtils.createDatastream(connectorType2, "name2", "source2");

    coordinator.validatePartitionAssignmentSupported(datastream1);

    try {
      coordinator.validatePartitionAssignmentSupported(datastream2);
      Assert.fail("Should fail validation when partition assignment is not supported");
    } catch (DatastreamValidationException e) {
      LOG.info("Caught exception as partition assignment is not supported");
    }
  }

  @Test
  public void testValidateDatastreamsUpdate() throws Exception {
    String testCluster = "testValidateDatastreamsUpdate";

    String connectorType1 = "connectorType1";
    String connectorType2 = "connectorType2";

    TestHookConnector connector1 = new TestHookConnector("connector1", connectorType1);
    TestHookConnector connector2 = new TestHookConnector("connector2", connectorType2);

    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);
    coordinator.addConnector(connectorType1, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.addConnector(connectorType2, connector2, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);
    DatastreamStore store = new ZookeeperBackedDatastreamStore(_cachedDatastreamReader, zkClient, testCluster);
    DatastreamResources resource = new DatastreamResources(store, coordinator);

    List<Datastream> datastreams = new ArrayList<>();

    datastreams.add(DatastreamTestUtils.createDatastream(connectorType1, "name1", "source1"));
    datastreams.add(DatastreamTestUtils.createDatastream(connectorType1, "name2", "source2"));
    datastreams.add(DatastreamTestUtils.createDatastream(connectorType2, "name3", "source3"));
    datastreams.add(DatastreamTestUtils.createDatastream(connectorType2, "name4", "source4"));

    datastreams.forEach(ds -> resource.create(ds));
    Assert.assertTrue(PollUtils.poll(() -> resource.getAll(new PagingContext(0, 4)).size() >= 4,
        100, 10000));

    try {
      BatchUpdateRequest<String, Datastream> batchRequest = new BatchUpdateRequest<>(
          datastreams.stream().collect(Collectors.toMap(Datastream::getName, ds -> ds)));
      resource.batchUpdate(batchRequest);
      Assert.fail("Should fail validation when there are multiple connector types");
    } catch (RestLiServiceException e) {
      LOG.info("Caught exception as expected on update of multiple connector types in one batch");
    }

    connector1._allowDatastreamUpdate = false;

    try {
      coordinator.validateDatastreamsUpdate(Arrays.asList(datastreams.get(0), datastreams.get(1)));
      Assert.fail("Should fail validation when update is not allowed");
    } catch (DatastreamValidationException e) {
      LOG.info("Caught exception as expected on update of connector type that doesn't support update");
    }

    coordinator.validateDatastreamsUpdate(Arrays.asList(datastreams.get(2), datastreams.get(3)));
  }

  @Test
  public void testCoordinatorHandleUpdateDatastream() throws Exception {
    String testCluster = "testCoordinatorHandleUpdateDatastream";

    String connectorType = "connectorType";

    TestHookConnector connector1 = new TestHookConnector("connector1", connectorType);
    TestHookConnector connector2 = new TestHookConnector("connector2", connectorType);

    Coordinator coordinator1 = createCoordinator(_zkConnectionString, testCluster);
    coordinator1.addConnector(connectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator1.start();

    Coordinator coordinator2 = createCoordinator(_zkConnectionString, testCluster);
    coordinator2.addConnector(connectorType, connector2, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator2.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    Datastream[] list =
        DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType, "datastream1");
    Datastream datastream = list[0];
    LOG.info("Created datastream: {}", datastream);

    // wait for datastream to be READY
    PollUtils.poll(() -> DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream1")
        .getStatus()
        .equals(DatastreamStatus.READY), 1000, WAIT_TIMEOUT_MS);
    datastream = DatastreamTestUtils.getDatastream(zkClient, testCluster, datastream.getName());
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastream.getName());
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, datastream.getName());

    // update datastream
    datastream.getMetadata().put("key", "value");
    datastream.getSource().setConnectionString("newSource");

    LOG.info("Updating datastream: {}", datastream);
    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(zkClient, testCluster);
    ZookeeperBackedDatastreamStore dsStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, testCluster);
    DatastreamResources datastreamResources = new DatastreamResources(dsStore, coordinator1);
    datastreamResources.update(datastream.getName(), datastream);

    assertConnectorReceiveDatastreamUpdate(connector1, datastream);
    assertConnectorReceiveDatastreamUpdate(connector2, datastream);
  }

  @Test
  public void testCoordinationMultipleConnectorTypesForBroadcastStrategy() throws Exception {
    String testCluster = "testCoordinationMultipleConnectors";

    String connectorType1 = "connectorType1";
    String connectorType2 = "connectorType2";

    LOG.info("create two coordinators and two connector instances per coordinator of broadcast strategy");

    //
    // create two live instances, each handle two different types of connectors
    //
    TestHookConnector connector11 = new TestHookConnector("connector11", connectorType1);
    TestHookConnector connector12 = new TestHookConnector("connector12", connectorType2);

    TestHookConnector connector21 = new TestHookConnector("connector21", connectorType1);
    TestHookConnector connector22 = new TestHookConnector("connector22", connectorType2);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    instance1.addConnector(connectorType1, connector11, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.addConnector(connectorType2, connector12, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    instance2.addConnector(connectorType1, connector21, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance2.addConnector(connectorType2, connector22, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance2.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create a datastream of connectorType1");

    //
    // create a new datastream for connectorType1
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType1, "datastream1");

    LOG.info("Verify whether the datastream is assigned to connector instances on both the coordinator");

    //
    // verify both live instances have tasks assigned for connector type 1 only
    //
    assertConnectorAssignment(connector11, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector12.getTasks().isEmpty());

    assertConnectorAssignment(connector21, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector22.getTasks().isEmpty());

    LOG.info("Create a datastream of connectorType2");

    //
    // create a new datastream for connectorType2
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType2, "datastream2");

    LOG.info("Verify the assignment");

    //
    // verify both live instances have tasks assigned for both connector types
    //
    assertConnectorAssignment(connector11, WAIT_TIMEOUT_MS, "datastream1");
    assertConnectorAssignment(connector12, WAIT_TIMEOUT_MS, "datastream2");

    assertConnectorAssignment(connector21, WAIT_TIMEOUT_MS, "datastream1");
    assertConnectorAssignment(connector22, WAIT_TIMEOUT_MS, "datastream2");

    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  //
  // stress test, start multiple coordinator instances at the same time, and make sure that all of them
  // will get a unique instance name
  //
  @Test(enabled = false)
  public void testStressLargeNumberOfLiveInstances() throws Exception {
    int concurrencyLevel = 100;
    String testCluster = "testStressUniqueInstanceNames";
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    // the duration of each live instance thread, make sure it is long enough to verify the result
    long duration = WAIT_DURATION_FOR_ZK;

    for (int i = 0; i < concurrencyLevel; i++) {
      Runnable task = () -> {
        // keep the thread alive
        try {
          Coordinator instance = createCoordinator(_zkConnectionString, testCluster);
          instance.start();

          Thread.sleep(duration);
          instance.stop();
        } catch (Exception ex) {
          LOG.error("Failed to launch coordinator", ex);
          Assert.fail();
        }
      };

      executor.execute(task);
    }

    //
    // wait for all instances go online
    //
    Thread.sleep(duration);

    //
    // verify all instances are alive
    //
    List<String> instances = zkClient.getChildren(KeyBuilder.liveInstances(testCluster));

    List<String> sortedList = new ArrayList<>(instances);
    Collections.sort(sortedList);
    LOG.info("Live instances {}", sortedList);

    Assert.assertEquals(instances.size(), concurrencyLevel, String.format("Live instances %s", sortedList));
    zkClient.close();
  }

  @Test
  public void testStressLargeNumberOfDatastreams() throws Exception {

    int concurrencyLevel = 10;

    String testCluster = "testStressLargeNumberOfDatastreams";
    String testConnectorType = "testConnectorType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create two coordinator and two connector instances of broadcast strategy");

    //
    // create 1 live instance and start it
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);

    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance2.start();

    String[] datastreamNames = new String[concurrencyLevel];

    LOG.info("Create 10 datastreams");

    //
    // create large number of datastreams
    //
    for (int i = 0; i < concurrencyLevel; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName + i);
      datastreamNames[i] = datastreamName + i;
    }

    LOG.info("validate whether all the datastreams are assigned to all the instances");

    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamNames);
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, datastreamNames);

    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  //
  // Test SimpleAssignmentStrategy: if new live instances come online, some tasks
  // will be moved from existing live instance to the new live instance
  //
  @Test
  public void testSimpleAssignmentReassignWithNewInstances() throws Exception {
    String testCluster = "testSimpleAssignmentReassignWithNewInstances";
    String testConnectorType = "testConnectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Creating the first coordinator and connector instance");
    //
    // create 1 instance
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance1.start();

    LOG.info("Creating two datastream");

    //
    // create 2 datastreams, [datastream0, datastream1]
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, "datastream0");
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, "datastream1");

    //
    // verify both datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream1");

    LOG.info("Creating the second coordinator and connector instance");

    //
    // add a new live instance instance2
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance2.start();

    //
    // verify new assignment. instance1 : [datastream0], instance2: [datastream1]
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1");

    LOG.info("Creating the third coordinator and connector instance");

    //
    // add instance3
    //
    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector("connector3", testConnectorType);
    instance3.addConnector(testConnectorType, connector3, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance3.start();

    //
    // verify assignment didn't change
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector3.getTasks().isEmpty());

    // Pause "datastream0"
    Datastream ds0 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream0");
    ds0.setStatus(DatastreamStatus.PAUSED);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds0);

    //
    // verify new assignment. instance1 : [datastream1], instance2: []
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector2.getTasks().isEmpty());
    Assert.assertTrue(connector3.getTasks().isEmpty());

    // Verify that the Tasks for Datastream0 are parked.
    String pausedPath = KeyBuilder.instanceAssignments(testCluster, Coordinator.PAUSED_INSTANCE);
    Assert.assertEquals(zkClient.getChildren(pausedPath).size(), 1);

    // Check that the task status is Paused
    String task0Name = zkClient.getChildren(pausedPath).get(0);
    String json =
        zkClient.readData(KeyBuilder.datastreamTaskStateKey(testCluster, testConnectorType, task0Name, "STATUS"), true);
    Assert.assertEquals(JsonUtils.fromJson(json, DatastreamTaskStatus.class), DatastreamTaskStatus.paused());

    // Resume "datastream0"
    ds0 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "datastream0");
    ds0.setStatus(DatastreamStatus.READY);
    DatastreamTestUtils.updateDatastreams(zkClient, testCluster, ds0);

    //
    // verify new assignment. instance1 : [datastream0], instance2: [datastream1]
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector3.getTasks().isEmpty());

    // Verify no Datastream Tasks Parked.
    Assert.assertEquals(zkClient.getChildren(pausedPath).size(), 0);

    Assert.assertEquals(connector1.getTasks().get(0).getStatus(), DatastreamTaskStatus.ok());

    System.out.println("XXXX   value :: " + connector1.getTasks().get(0).getStatus());
    //
    // clean up
    //
    zkClient.close();
    instance1.stop();
    instance2.stop();
    instance3.stop();
  }

  //
  // Test for SimpleAssignmentStrategy
  // Verify that when instance dies, the assigned tasks will be re-assigned to remaining live instances
  //
  @Test
  public void testSimpleAssignmentReassignAfterDeath() throws Exception {
    String testCluster = "testSimpleAssignmentReassignAfterDeath";
    String testConnectorType = "testConnectorType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create two coordinators and two connector instances");

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance1.start();

    // make sure the instance2 can be taken offline cleanly with session expiration
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance2.start();

    LOG.info("Create four datastreams");

    //
    // create 4 datastreams, [datastream0, datastream1, datastream2, datastream3]
    //
    for (int i = 0; i < 4; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName + i);
    }

    LOG.info("Verify that the datastreams are assigned across two connectors");

    waitTillAssignmentIsComplete(4, WAIT_TIMEOUT_MS, connector1, connector2);
    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, WAIT_DURATION_FOR_ZK, "datastream0", "datastream2");
    assertConnectorAssignment(connector2, WAIT_DURATION_FOR_ZK, "datastream1", "datastream3");

    LOG.info("Tasks assigned to instance1: " + connector1.getTasks().toString());
    LOG.info("Tasks assigned to instance2: " + connector2.getTasks().toString());

    List<DatastreamTask> tasks1 = new ArrayList<>(connector1.getTasks());
    tasks1.addAll(connector2.getTasks());
    Collections.sort(tasks1, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Take the instance2 offline");

    //
    // take instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    LOG.info("verify that the four datastreams are assigned to the instance1");

    //
    // verify all 4 datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream1", "datastream2", "datastream3");

    // Make sure strategy reused all tasks as opposed to creating new ones
    List<DatastreamTask> tasks2 = new ArrayList<>(connector1.getTasks());
    Collections.sort(tasks2, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Tasks1: " + tasks1.toString());
    LOG.info("Tasks2: " + tasks2.toString());

    Assert.assertEquals(tasks1, tasks2);

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
  }

  @Test
  public void testBroadcastAssignmentReassignAfterDeath() throws Exception {
    String testCluster = "testBroadcastAssignmentReassignAfterDeath";
    String testConnectorType = "testConnectorType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Creating two coordinator and two connector instances");

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    // make sure the instance2 can be taken offline cleanly with session expiration
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance2.start();

    LOG.info("Create two datastreams");

    //
    // create 2 datastreams, [datastream0, datastream1]
    //
    for (int i = 0; i < 2; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName + i);
    }

    LOG.info("Validate the broadcast assignment");

    //
    // verify assignment, instance1: [datastream0, datastream1], instance2:[datastream0, datastream1]
    //
    assertConnectorAssignment(connector1, WAIT_DURATION_FOR_ZK, "datastream0", "datastream1");
    assertConnectorAssignment(connector2, WAIT_DURATION_FOR_ZK, "datastream0", "datastream1");

    List<DatastreamTask> tasks2 = new ArrayList<>(connector2.getTasks());

    LOG.info("Take the instance2 offline");

    //
    // take instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    LOG.info("Verify whether the live instance assignment has been removed.");

    // Verify dead instance assignments have been removed
    String instancePath = KeyBuilder.instanceAssignments(testCluster, instance2.getInstanceName());
    Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(instancePath), 200, WAIT_TIMEOUT_MS));

    LOG.info("Verify instance1 still has two datastreams");

    //
    // verify instance1 still has 2 datastreams
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream1");

    // Make sure Coordinator has removed deprecated connector tasks of instance2
    for (DatastreamTask task : tasks2) {
      String path = KeyBuilder.connectorTask(testCluster, task.getConnectorType(), task.getDatastreamTaskName());
      LOG.info("Checking whether the path doesn't exist anymore: " + path);
      Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(path), 200, WAIT_TIMEOUT_MS));
    }

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
  }

  //
  // this case tests the scenario when the leader of the cluster dies, and make sure
  // the assignment will be taken over by the new leader.
  //
  @Test
  public void testSimpleAssignmentReassignAfterLeaderDeath() throws Exception {
    String testCluster = "testSimpleAssignmentReassignAfterLeaderDeath";
    String testConnectorType = "testConnectorType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Creating three coordinator and connector instances ");
    //
    // setup a cluster with 3 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance2.start();

    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector("connector3", testConnectorType);
    instance3.addConnector(testConnectorType, connector3, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance3.start();

    LOG.info("Creating six datastreams");
    //
    // create 6 datastreams, [datastream0, ..., datastream5]
    //
    for (int i = 0; i < 6; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName + i);
    }

    LOG.info("Verify whether the six datastreams are assigned to the three connector instances");

    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, WAIT_DURATION_FOR_ZK, "datastream0", "datastream3");
    assertConnectorAssignment(connector2, WAIT_DURATION_FOR_ZK, "datastream1", "datastream4");
    assertConnectorAssignment(connector3, WAIT_DURATION_FOR_ZK, "datastream2", "datastream5");

    List<DatastreamTask> tasks1 = new ArrayList<>(connector1.getTasks());
    tasks1.addAll(connector2.getTasks());
    tasks1.addAll(connector3.getTasks());
    Collections.sort(tasks1, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Stop the instance1 and delete the live instance");

    //
    // take current leader instance1 offline
    //
    instance1.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance1);

    LOG.info("Verify that the 6 datastreams are assigned to the remaining two instances");

    //
    // verify all 6 datastreams are assigned to instance2 and instance3
    //
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream0", "datastream2", "datastream4");
    assertConnectorAssignment(connector3, WAIT_TIMEOUT_MS, "datastream1", "datastream3", "datastream5");

    LOG.info("Stop the instance2 and delete the live instance");

    //
    // take current leader instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    LOG.info("Verify that the 6 datastreams are assigned to remaining one instance");

    //
    // verify all tasks assigned to instance3
    assertConnectorAssignment(connector3, WAIT_TIMEOUT_MS, "datastream0", "datastream2", "datastream4", "datastream1",
        "datastream3", "datastream5");

    LOG.info("Make sure strategy reused all the tasks as opposed to creating new ones");

    // Make sure strategy reused all tasks as opposed to creating new ones
    List<DatastreamTask> tasks2 = new ArrayList<>(connector3.getTasks());
    Collections.sort(tasks2, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Tasks1: " + tasks1.toString());
    LOG.info("Tasks2: " + tasks2.toString());

    Assert.assertEquals(tasks1, tasks2);

    // Verify dead instance assignments have been removed
    Assert.assertTrue(!zkClient.exists(KeyBuilder.instanceAssignments(testCluster, instance1.getInstanceName())));
    Assert.assertTrue(!zkClient.exists(KeyBuilder.instanceAssignments(testCluster, instance2.getInstanceName())));

    //
    // clean up
    //
    instance3.stop();
    zkClient.close();
  }

  //
  // this test covers the scenario when multiple instances die at the same time
  //
  @Test
  public void testMultipleInstanceDeath() throws Exception {
    String testCluster = "testMultipleInstanceDeath";
    String testConnectorType = "testConnectorType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create four instances");
    //
    // create a list of instances
    //
    int count = 4;
    Coordinator[] coordinators = new Coordinator[count];
    TestHookConnector[] connectors = new TestHookConnector[count];
    for (int i = 0; i < count; i++) {
      coordinators[i] = createCoordinator(_zkConnectionString, testCluster);
      connectors[i] = new TestHookConnector("connector" + i, testConnectorType);
      coordinators[i].addConnector(testConnectorType, connectors[i], new LoadbalancingStrategy(), false,
          new SourceBasedDeduper(), null);
      coordinators[i].start();
    }

    LOG.info("Create four datastreams");

    //
    // create 1 datastream per instance
    //
    for (int i = 0; i < count; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, datastreamName + i);
    }

    LOG.info("Validate whether the four datastreams are assigned to four instances");

    //
    // wait until the last instance was assigned the last datastream, by now all datastream should be assigned
    //
    assertConnectorAssignment(connectors[count - 1], WAIT_TIMEOUT_MS, "datastream" + (count - 1));

    LOG.info("kill three instances except for the leader");

    //
    // kill all instances except the current leader
    //
    for (int i = 1; i < count; i++) {
      coordinators[i].stop();
      deleteLiveInstanceNode(zkClient, testCluster, coordinators[i]);
    }

    LOG.info("Check whether all the instances are assigned to the only remaining instance.");

    //
    // validate all datastream tasks are assigned to the leader now
    //
    String[] assignment = new String[count];
    for (int i = 0; i < count; i++) {
      assignment[i] = datastreamName + i;
    }
    assertConnectorAssignment(connectors[0], WAIT_TIMEOUT_MS, assignment);

    //
    // clean up
    //
    coordinators[0].stop();
    zkClient.close();
  }

  //
  // Test SimpleAssignmentStrategy, verify that the assignment is predictable no matter what the datastreams
  // are. This is because the assignment strategy will sort the datastreams by names. If a new datastream
  // has a smaller lexicographical order, it will be assigned to an instance with smaller lexicographical order.
  // Put it in another word, this is how Kafka consumer rebalancing works.
  //
  @Test
  public void testSimpleAssignmentRebalancing() throws Exception {
    String testCluster = "testSimpleAssignmentRebalancing";
    String testConnectorType = "testConnectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create two coordinators and connector instances");

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectorType);
    instance1.addConnector(testConnectorType, connector1, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectorType);
    instance2.addConnector(testConnectorType, connector2, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance2.start();

    LOG.info("Create two datastreams.");

    //
    // create 2 datastreams [datastream1, datastream2]
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, "datastream1");
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, "datastream2");

    LOG.info("verify that two datastreams are assigned to two instances");

    //
    // verify assignment instance1: [datastream1], instance2:[datastream2]
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream1");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream2");

    LOG.info("create a third datastream but which is smaller than earlier two");
    //
    // create 1 new datastream "datastream0", which has the smallest lexicographical order
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectorType, "datastream0");

    LOG.info("Verify that the new datastream is assigned to first instance.");

    //
    // verify assignment instance1:[datastream0, datastream2], instance2:[datastream1]
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream2");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1");

    //
    // clean up
    //
    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  //
  // Verify each connector type has its assignment strategy and it is executed independently
  // That is, the assignment of one connector type will not affect the assignment of the
  // other type, even though the assignment strategies are different. In this test case,
  // we have two connectors for each instance, and they are using different assignment
  // strategies, BroadcastStrategy and SimpleStrategy respectively.
  //
  @Test
  public void testSimpleAssignmentStrategyIndependent() throws Exception {
    String testCluster = "testSimpleAssignmentStrategy";
    String connectorType1 = "ConnectorType1";
    String connectorType2 = "ConnectorType2";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info(
        "Create two coordinator with two connector types (one simple and one broadcast) in each and create a connector instance"
            + " of each connector type per coordinator");

    //
    // setup a cluster with 2 live instances with simple assignment strategy,
    // each has two connectors
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1a = new TestHookConnector("connector1a", connectorType1);
    TestHookConnector connector1b = new TestHookConnector("connector1b", connectorType2);
    instance1.addConnector(connectorType1, connector1a, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance1.addConnector(connectorType2, connector1b, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2a = new TestHookConnector("connector2a", connectorType1);
    TestHookConnector connector2b = new TestHookConnector("connector2b", connectorType2);
    instance2.addConnector(connectorType1, connector2a, new LoadbalancingStrategy(), false, new SourceBasedDeduper(),
        null);
    instance2.addConnector(connectorType2, connector2b, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance2.start();

    LOG.info("Create three datastreams of connectorType1 and three datastreams of connectorType2");

    //
    // create 3 datastreams ["simple0", "simple1", "simple2"] for ConnectoryType1
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType1, "simple0", "simple1",
        "simple2");
    //
    // create 3 datastreams [datastream2, datastream3, datastream4] for ConnectorType2
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType2, "broadcast0", "broadcast1",
        "broadcast2");

    LOG.info("verify that the datastreams are properly assigned based on simple or broadcast");
    //
    // verify assignment: instance1.connector1: [datastream0], connector2:[datastream2, datastream4"]
    // instance2.connector1:[datastream1], connector2:[datastream3]
    //
    assertConnectorAssignment(connector1a, WAIT_TIMEOUT_MS, "simple0", "simple2");
    assertConnectorAssignment(connector1b, WAIT_TIMEOUT_MS, "broadcast0", "broadcast1", "broadcast2");
    assertConnectorAssignment(connector2a, WAIT_TIMEOUT_MS, "simple1");
    assertConnectorAssignment(connector2b, WAIT_TIMEOUT_MS, "broadcast0", "broadcast1", "broadcast2");

    //
    // clean up
    //
    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  @Test
  public void testCoordinatorErrorHandling() throws Exception {
    String testCluster = "testCoordinatorErrorHandling";
    String connectorType1 = "ConnectorType1";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    BadConnector connector1 = new BadConnector();
    instance1.addConnector(connectorType1, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    instance1.start();

    //
    // validate the error nodes has 0 child because onAssignmentChange is not triggered yet
    //
    String errorPath = KeyBuilder.instanceErrors(testCluster, instance1.getInstanceName());
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 0, 500, WAIT_TIMEOUT_MS);

    //
    // create a new datastream, which will trigger the error path
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType1, "datastream0");

    //
    // validate the error nodes now has 1 child
    //
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 1, 500, WAIT_TIMEOUT_MS);

    //
    // create another datastream, and validate the error nodes have 2 children
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType1, "datastream1");
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 2, 500, WAIT_TIMEOUT_MS);

    //
    // create 10 more datastream, and validate the error children is caped at 10
    //
    for (int i = 2; i < 12; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType1, "datastream" + i);
    }
    PollUtils.poll(() -> connector1.getAssignmentCount() == 12, 200, WAIT_TIMEOUT_MS);
    int childrenCount = zkClient.countChildren(errorPath);
    Assert.assertTrue(childrenCount <= 10);

    //
    // clean up
    //
    zkClient.close();
    instance1.stop();
  }

  private void doTestTaskAssignmentAfterDestinationDedupe(String testName, boolean compat) throws Exception {
    String testCluster = testName;
    String connectorName = "TestConnector";
    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector = new TestHookConnector("connector1", connectorName);
    coordinator.addConnector(connectorName, connector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator.start();

    // Create 1st datastream
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    Datastream stream1 =
        DatastreamTestUtils.createAndStoreDatastreams(zkClient, testName, connectorName, "stream54321")[0];
    Assert.assertTrue(stream1.getMetadata().containsKey(DatastreamMetadataConstants.CREATION_MS));

    // Wait for first assignment is done
    Assert.assertTrue(PollUtils.poll(() -> connector.getTasks().size() == 1, 50, WAIT_TIMEOUT_MS));

    Meter numRebals = ReflectionUtils.getField(coordinator, "_numRebalances");
    Assert.assertNotNull(numRebals);

    long numAssign1 = numRebals.getCount();
    DatastreamTask task1 = connector.getTasks().get(0);

    if (compat) {
      // Remove timestamp for compat mode testing
      CachedDatastreamReader reader = ReflectionUtils.getField(coordinator, "_datastreamCache");
      Assert.assertNotNull(reader);
      reader.getDatastream(stream1.getName(), false).getMetadata().remove(DatastreamMetadataConstants.CREATION_MS);
    }

    // Create 2nd datastream with name alphabetically "smaller" than 1st datastream and same destination
    Datastream stream2 = stream1.clone();
    stream2.setName("stream12345");
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, stream2);

    // Wait for new assignment is done
    Assert.assertTrue(PollUtils.poll(() -> (numRebals.getCount() > numAssign1), 50, WAIT_TIMEOUT_MS));

    // Make sure connector still has only one task
    Assert.assertEquals(connector.getTasks().size(), 1);

    Assert.assertEquals(connector.getTasks().get(0), task1);
  }

  /**
   * Test the scenario where a newer datastream shares the destination as an existing
   * datastream but the name of the new datastream is alphabetically "smaller" than
   * the existing datastream. In this case, the existing datastream should be chosen
   * for task assignment after de-dup, thus without resulting in new tasks created.
   */
  @Test(enabled = false)
  public void testTaskAssignmentAfterDestinationDedupe() throws Exception {
    doTestTaskAssignmentAfterDestinationDedupe("testTaskAssignmentAfterDestinationDedupe", false);
  }

  /**
   * Test the same scenario as testTaskAssignmentAfterDestinationDedupe but ensure
   * compatibility with existing datastreams without the timestamp in metadata.
   * In which case, the datastream without timestamp metadata is the older one.
   */
  @Test(enabled = false)
  public void testTaskAssignmentAfterDestinationDedupeCompat() throws Exception {
    doTestTaskAssignmentAfterDestinationDedupe("testTaskAssignmentAfterDestinationDedupeCompat", true);
  }

  private TestSetup createTestCoordinator() throws Exception {
    EmbeddedDatastreamCluster datastreamKafkaCluster =
        TestDatastreamServer.initializeTestDatastreamServerWithDummyConnector(null);
    datastreamKafkaCluster.startup();
    Properties properties = datastreamKafkaCluster.getDatastreamServerProperties().get(0);
    DatastreamResources resource = new DatastreamResources(datastreamKafkaCluster.getPrimaryDatastreamServer());

    Coordinator coordinator =
        createCoordinator(properties.getProperty(DatastreamServerConfigurationConstants.CONFIG_ZK_ADDRESS),
            properties.getProperty(DatastreamServerConfigurationConstants.CONFIG_CLUSTER_NAME));

    TestHookConnector connector = new TestHookConnector("connector1", DummyConnector.CONNECTOR_TYPE);

    coordinator.addConnector(DummyConnector.CONNECTOR_TYPE, connector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);

    coordinator.start();

    return new TestSetup(datastreamKafkaCluster, coordinator, resource, connector);
  }

  private void validateRetention(Datastream stream, DatastreamResources resource, Duration expectedRetention) {
    Datastream queryStream = resource.get(stream.getName());
    Assert.assertNotNull(queryStream.getDestination());
    StringMap metadata = queryStream.getMetadata();
    Assert.assertNotNull(metadata.getOrDefault(DatastreamMetadataConstants.DESTINATION_CREATION_MS, null));
    Assert.assertNotNull(metadata.getOrDefault(DatastreamMetadataConstants.DESTINATION_RETENTION_MS, null));
    String retentionMs = metadata.get(DatastreamMetadataConstants.DESTINATION_RETENTION_MS);
    Assert.assertEquals(retentionMs, String.valueOf(expectedRetention.toMillis()));
  }

  /**
   * Test create and delete datastream scenario with the actual DSM.
   *
   * The expected outcome includes:
   *
   * 1) a new datastream is created by DSM and can be queried by name afterwards
   * 2) the datastream has valid destination (populated by DestinationManager)
   * 3) connector is assigned the task for the datastream
   * 4) the data stream is deleted with proper clean-up
   */
  @Test
  public void testCreateAndDeleteDatastreamHappyPath() throws Exception {
    TestSetup setup = createTestCoordinator();

    String datastreamName = "TestDatastream";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    stream.getDestination()
        .setConnectionString(new KafkaDestination(setup._datastreamKafkaCluster.getKafkaCluster().getZkConnection(),
            "TestDatastreamTopic", false).getDestinationURI());
    CreateResponse createResponse = setup._resource.create(stream);
    Assert.assertNull(createResponse.getError());
    Assert.assertEquals(createResponse.getStatus(), HttpStatus.S_201_CREATED);

    // Make sure connector has received the assignment (timeout in 30 seconds)
    assertConnectorAssignment(setup._connector, 30000, datastreamName);

    // Delete the data stream and verify proper cleanup
    UpdateResponse deleteResponse = setup._resource.delete(stream.getName());
    Assert.assertEquals(deleteResponse.getStatus(), HttpStatus.S_200_OK);
    assertConnectorAssignment(setup._connector, 30000);
  }

  @Test
  public void testCreateDatastreamHappyPathDefaultRetention() throws Exception {
    TestSetup setup = createTestCoordinator();

    // Check default retention when no topicConfig is specified
    String datastreamName = "TestDatastream";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);

    stream.getDestination()
        .setConnectionString(new KafkaDestination(setup._datastreamKafkaCluster.getKafkaCluster().getZkConnection(),
            "TestDatastreamTopic", false).getDestinationURI());

    CreateResponse response = setup._resource.create(stream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Make sure connector has received the assignment (timeout in 30 seconds)
    assertConnectorAssignment(setup._connector, 30000, datastreamName);
    validateRetention(stream, setup._resource, KafkaTransportProviderAdmin.DEFAULT_RETENTION);

    setup._datastreamKafkaCluster.shutdown();
  }

  @Test
  public void testHeartbeat() throws Exception {
    // Use 1s heartbeat period for quicker execution
    Properties override = new Properties();
    override.put("brooklin.server.coordinator.heartbeatPeriodMs", "1000");

    Coordinator coordinator = createCoordinator(_zkConnectionString, "testHeartbeat", override);

    // Mock the DMM to avoid interference from other test cases
    DynamicMetricsManager dynMM = mock(DynamicMetricsManager.class);

    AtomicLong counter = new AtomicLong();
    doAnswer((invocation) -> {
      String metricName = (String) invocation.getArguments()[1];
      if (metricName.equalsIgnoreCase("numHeartbeats")) {
        counter.incrementAndGet();
      }
      return null;
    }).when(dynMM).createOrUpdateCounter(anyString(), anyObject(), anyLong());

    ReflectionUtils.setField(coordinator, "_dynamicMetricsManager", dynMM);

    coordinator.start();

    // Wait up to 30s for the first heartbeat
    Assert.assertTrue(PollUtils.poll(() -> counter.get() >= 1, 1000, 30000));
  }

  @Test
  public void testIsLeader() throws Exception {
    String testCluster = "testIsLeader";
    String isLeaderMetricName = "Coordinator.isLeader";

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    instance1.start();
    Assert.assertTrue(instance1.getIsLeader().getAsBoolean());
    Gauge<Integer> isLeader = DynamicMetricsManager.getInstance().getMetric(isLeaderMetricName);
    Assert.assertEquals(isLeader.getValue().intValue(), 1);

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    instance2.start();

    Assert.assertTrue(instance1.getIsLeader().getAsBoolean());
    Assert.assertFalse(instance2.getIsLeader().getAsBoolean());

    instance1.stop();
    Assert.assertTrue(PollUtils.poll(() -> instance2.getIsLeader().getAsBoolean(), 100, 30000));
  }

  @Test
  public void testDatastreamAuthorizationHappyPath() throws Exception {
    createTestCoordinator();
    Authorizer authz = mock(Authorizer.class);
    when(authz.authorize(anyObject(), anyObject(), anyObject())).thenReturn(true);

    // Check default retention when no topicConfig is specified
    String datastreamName = "testDatastreamAuthorization";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);

    Assert.assertTrue(authz.authorize(stream, Authorizer.Operation.READ, () -> "dummy"));
  }

  @Test
  public void testDatastreamAuthorizationUnhappyPath() throws Exception {
    createTestCoordinator();
    Authorizer authz = mock(Authorizer.class);

    // Check default retention when no topicConfig is specified
    String datastreamName = "testDatastreamAuthorization";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);

    Assert.assertFalse(authz.authorize(stream, Authorizer.Operation.READ, () -> "dummy"));
  }

  @Test
  public void testDatastreamDeleteUponTTLExpire() throws Exception {
    TestSetup setup = createTestCoordinator();

    String[] streamNames = {"TestDatastreamTTLExpire1", "TestDatastreamTTLExpire2"};
    Datastream[] streams = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, streamNames);
    streams[0].getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    streams[1].getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    streams[0].getDestination()
        .setConnectionString(new KafkaDestination(setup._datastreamKafkaCluster.getKafkaCluster().getZkConnection(),
            "TestDatastreamTopic1", false).getDestinationURI());
    streams[1].getDestination()
        .setConnectionString(new KafkaDestination(setup._datastreamKafkaCluster.getKafkaCluster().getZkConnection(),
            "TestDatastreamTopic2", false).getDestinationURI());

    // stream1 expires after 500ms and should get deleted when stream2 is created
    long threeDaysAgo = Instant.now().minus(Duration.ofDays(3)).toEpochMilli();
    streams[0].getMetadata().put(CREATION_MS, String.valueOf(threeDaysAgo));
    long oneDayTTLMs = Duration.ofDays(1).toMillis();
    streams[0].getMetadata().put(TTL_MS, String.valueOf(oneDayTTLMs));

    // Creation should go through as TTL is not considered for freshly created streams (INITIALIZING)
    CreateResponse createResponse = setup._resource.create(streams[0]);
    Assert.assertNull(createResponse.getError());
    Assert.assertEquals(createResponse.getStatus(), HttpStatus.S_201_CREATED);

    // Creating a stream2 which should trigger stream1 to be deleted
    createResponse = setup._resource.create(streams[1]);
    Assert.assertNull(createResponse.getError());
    Assert.assertEquals(createResponse.getStatus(), HttpStatus.S_201_CREATED);

    // Poll up to 30s for stream1 to get deleted
    PollUtils.poll(() -> setup._resource.get(streams[0].getName()) == null, 200, Duration.ofSeconds(30).toMillis());
  }

  @Test
  public void testDoNotAssignExpiredStreams() throws Exception {
    TestSetup setup = createTestCoordinator();

    String[] streamNames = {"TestDatastreamTTLExpire1", "TestDatastreamTTLExpire2"};
    Datastream[] streams = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, streamNames);
    streams[0].getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    streams[1].getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    streams[0].getDestination()
        .setConnectionString(new KafkaDestination(setup._datastreamKafkaCluster.getKafkaCluster().getZkConnection(),
            "TestDatastreamTopic1", false).getDestinationURI());
    streams[1].getDestination()
        .setConnectionString(new KafkaDestination(setup._datastreamKafkaCluster.getKafkaCluster().getZkConnection(),
            "TestDatastreamTopic2", false).getDestinationURI());

    // stream2 expires after 500ms and should not get assigned
    long threeDaysAgo = Instant.now().minus(Duration.ofDays(3)).toEpochMilli();
    streams[1].getMetadata().put(CREATION_MS, String.valueOf(threeDaysAgo));
    long oneDayTTLMs = Duration.ofDays(1).toMillis();
    streams[1].getMetadata().put(TTL_MS, String.valueOf(oneDayTTLMs));

    // Creation of stream1 should go through
    CreateResponse createResponse = setup._resource.create(streams[0]);
    Assert.assertNull(createResponse.getError());
    Assert.assertEquals(createResponse.getStatus(), HttpStatus.S_201_CREATED);

    // Creating a stream2 which should go through as TTL is not considered for freshly created streams (INITIALIZING)
    createResponse = setup._resource.create(streams[1]);
    Assert.assertNull(createResponse.getError());
    Assert.assertEquals(createResponse.getStatus(), HttpStatus.S_201_CREATED);

    // Only stream1 should have been assigned as stream2 is expired already
    assertConnectorAssignment(setup._connector, WAIT_TIMEOUT_MS, streamNames[0]);
  }

  @Test
  public void testCachedDatastreamReader() throws Exception {
    TestSetup setup = createTestCoordinator();
    String testCluster = setup._coordinator.getClusterName();

    String[] streamNames = {"testCachedDatastreamReader1", "testCachedDatastreamReader2"};
    ZkClient zkClient = new ZkClient(setup._datastreamKafkaCluster.getZkConnection());

    CachedDatastreamReader reader = new CachedDatastreamReader(zkClient, testCluster);
    Assert.assertTrue(reader.getAllDatastreams().isEmpty());
    Assert.assertTrue(reader.getAllDatastreams(false).isEmpty());
    Assert.assertTrue(reader.getAllDatastreams(true).isEmpty());
    Assert.assertTrue(reader.getAllDatastreamNames().isEmpty());
    Assert.assertTrue(reader.getDatastreamGroups().isEmpty());
    Assert.assertNull(reader.getDatastream("foo", true));

    Datastream[] streams =
        DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, DummyConnector.CONNECTOR_TYPE,
            streamNames);

    // Flush and cache should be populated
    Assert.assertFalse(reader.getAllDatastreams(true).isEmpty());
    Assert.assertFalse(reader.getAllDatastreams().isEmpty());
    Assert.assertFalse(reader.getAllDatastreamNames().isEmpty());
    Assert.assertFalse(reader.getDatastreamGroups().isEmpty());

    Assert.assertNotNull(reader.getDatastream("testCachedDatastreamReader1", false));
    Assert.assertNotNull(reader.getDatastream("testCachedDatastreamReader2", false));

    Assert.assertTrue(PollUtils.poll(() -> setup._resource.getAll(new PagingContext(0, 2)).size() == 2,
        100, 10000), "Timed out waiting for ZkClients to sync up on datastream created");

    // Update stream2 to test cache invalidation
    streams[1].getMetadata().put("owner", "foo222");
    setup._resource.update("testCachedDatastreamReader2", streams[1]);

    reader.invalidateAllCache();

    // After invalidation, cache should be re-populated
    Assert.assertFalse(reader.getAllDatastreams().isEmpty());
    Assert.assertFalse(reader.getAllDatastreamNames().isEmpty());
    Assert.assertFalse(reader.getDatastreamGroups().isEmpty());
    Assert.assertNotNull(reader.getDatastream("testCachedDatastreamReader1", false));
    Assert.assertNotNull(reader.getDatastream("testCachedDatastreamReader2", false));
    Assert.assertEquals(reader.getDatastream("testCachedDatastreamReader2", false).getMetadata().get("owner"),
        "foo222");

    // Delete one stream
    setup._resource.delete("testCachedDatastreamReader1");

    // Make sure datastream is indeed deleted from ZK
    // _resource.delete only mark it as DELETING and only
    // later Coordinator does the actual deletion from ZK
    // through ZkAdapter.
    String path = KeyBuilder.datastream(testCluster, "testCachedDatastreamReader1");
    Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(path), 200, WAIT_TIMEOUT_MS));

    // Even without flush, testCachedDatastreamReader1 should eventually be evicted from cache
    Assert.assertTrue(
        PollUtils.poll(() -> reader.getDatastream("testCachedDatastreamReader1", false) == null, 200, WAIT_TIMEOUT_MS));
    Assert.assertFalse(reader.getAllDatastreams(false).isEmpty());
    Assert.assertEquals(reader.getDatastreamGroups().size(), 1);
  }

  @Test
  public void testPauseResumeSourcePartitions() throws Exception {
    String testCluster = "testCoordinatorHandleUpdateDatastream";
    String mmConnectorType = "mmConnectorType";

    MMTestHookConnector mmConnector = new MMTestHookConnector("mmConnector", mmConnectorType);
    Coordinator mmCoordinator = createCoordinator(_zkConnectionString, testCluster);
    mmCoordinator.addConnector(mmConnectorType, mmConnector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    mmCoordinator.start();
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    // Create mm datastream
    Datastream[] list =
        DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, mmConnectorType, "mmDatastream");
    Datastream mmDatastream = list[0];
    LOG.info("Created datastream: {}", mmDatastream);

    // wait for datastream to be READY
    PollUtils.poll(() -> DatastreamTestUtils.getDatastream(zkClient, testCluster, "mmDatastream")
        .getStatus()
        .equals(DatastreamStatus.READY), 1000, WAIT_TIMEOUT_MS);

    // Verify connector assignment
    mmDatastream = DatastreamTestUtils.getDatastream(zkClient, testCluster, mmDatastream.getName());
    assertConnectorAssignment(mmConnector, WAIT_TIMEOUT_MS, mmDatastream.getName());

    // Pause topics in mm datastream
    StringMap pausedPartitions = new StringMap();
    pausedPartitions.put("topic1", "*");
    pausedPartitions.put("topic2", "0,1");

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(mmDatastream.getName());

    LOG.info("calling pause on mm datastream: {}", mmDatastream);
    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(zkClient, testCluster);
    ZookeeperBackedDatastreamStore dsStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, testCluster);
    DatastreamResources datastreamResources = new DatastreamResources(dsStore, mmCoordinator);
    datastreamResources.pauseSourcePartitions(pathKey, pausedPartitions);
    mmDatastream = DatastreamTestUtils.getDatastream(zkClient, testCluster, mmDatastream.getName());
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(mmDatastream),
        DatastreamUtils.parseSourcePartitionsStringMap(pausedPartitions));

    assertConnectorReceiveDatastreamUpdate(mmConnector, mmDatastream);
    DatastreamTask task = mmConnector.getDatastreamTask(mmDatastream.getName());

    // Make sure it received partitions to pause
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(task.getDatastreams()
        .get(0)),
        DatastreamUtils.parseSourcePartitionsStringMap(pausedPartitions));
  }

  // Make sure mirror maker operations are prohibited for others
  @Test
  public void testPauseResumeSourcePartitionsThrowsErrorForNonMMConnectors() throws Exception {

    String testCluster = "testCoordinatorHandleUpdateDatastream";

    String nonMmConnectorType = "nonMmConnectorType";

    TestHookConnector nonMmConnector = new TestHookConnector("nonMmConnector", nonMmConnectorType);

    Coordinator nonMmCoordinator = createCoordinator(_zkConnectionString, testCluster);
    nonMmCoordinator.addConnector(nonMmConnectorType, nonMmConnector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    nonMmCoordinator.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    // Create nonMm datastream
    Datastream[] list =
        DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, nonMmConnectorType, "nonMmDatastream");
    Datastream nonMmDatastream = list[0];
    LOG.info("Created datastream: {}", nonMmDatastream);

    // wait for datastream to be READY
    PollUtils.poll(() -> DatastreamTestUtils.getDatastream(zkClient, testCluster, "nonMmDatastream")
        .getStatus()
        .equals(DatastreamStatus.READY), 1000, WAIT_TIMEOUT_MS);

    // Verify connector assignment
    nonMmDatastream = DatastreamTestUtils.getDatastream(zkClient, testCluster, nonMmDatastream.getName());
    assertConnectorAssignment(nonMmConnector, WAIT_TIMEOUT_MS, nonMmDatastream.getName());

    // Create datastream resource
    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(zkClient, testCluster);
    ZookeeperBackedDatastreamStore dsStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, testCluster);
    DatastreamResources datastreamResources = new DatastreamResources(dsStore, nonMmCoordinator);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(nonMmDatastream.getName());

    // Create some paused partitions stringmap
    // Doesn't matter the value for this test.
    StringMap pausedPartitions = new StringMap();

    // For non-mm, should receive an error
    boolean exceptionReceived = false;

    LOG.info("calling non mm datastream: {}", nonMmDatastream);
    datastreamCache = new CachedDatastreamReader(zkClient, testCluster);
    dsStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, testCluster);
    datastreamResources = new DatastreamResources(dsStore, nonMmCoordinator);
    try {
      datastreamResources.pauseSourcePartitions(pathKey, pausedPartitions);
    } catch (Exception e) {
      exceptionReceived = true;
    }
    Assert.assertTrue(exceptionReceived);
  }

  @Test
  public void testDatastreamDedupeMetadataCopy() throws Exception {
    String testCluster = "testDatastreamDedupeMetadataCopy";

    String connectorType = "connectorType";

    TestHookConnector connector1 = new TestHookConnector("connector1", connectorType);

    Coordinator coordinator1 = createCoordinator(_zkConnectionString, testCluster);
    coordinator1.addConnector(connectorType, connector1, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    coordinator1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    Datastream [] datastreams = DatastreamTestUtils.createDatastreams(connectorType, "stream1", "stream2");

    // Add extra destination metadata to stream0
    String destMetaKey = SYSTEM_DESTINATION_PREFIX + "foo";
    String destMetaVal = "bar";
    datastreams[0].getMetadata().put(destMetaKey, destMetaVal);
    datastreams[0].getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, "MyPrefix");

    // Store stream0 first
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, datastreams[0]);
    // wait for stream0 to be READY
    PollUtils.poll(() -> DatastreamTestUtils.getDatastream(zkClient, testCluster, "stream1")
        .getStatus()
        .equals(DatastreamStatus.READY), 1000, WAIT_TIMEOUT_MS);
    LOG.info("Created datastream: {}", datastreams[0]);

    // Trigger the code that does the dedupe and metadata copying
    datastreams[1].setSource(datastreams[0].getSource());
    datastreams[1].removeDestination();
    coordinator1.initializeDatastream(datastreams[1]);

    // Ensure both streams have the same destination (are deduped)
    Datastream stream1 = DatastreamTestUtils.getDatastream(zkClient, testCluster, "stream1");
    Assert.assertEquals(stream1.getDestination(), datastreams[1].getDestination());

    // Actually store stream2
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, datastreams[1]);

    // Wait until stream2 is fully initialized
    PollUtils.poll(() -> {
      datastreams[1] = DatastreamTestUtils.getDatastream(zkClient, testCluster, "stream2");
      return datastreams[1].getStatus() == DatastreamStatus.READY;
    }, 1000, WAIT_TIMEOUT_MS);

    // Ensure all destination-related metadata are copied into the deduped stream
    stream1.getMetadata().entrySet()
        .stream()
        .filter(e -> e.getKey().startsWith(DatastreamMetadataConstants.SYSTEM_DESTINATION_PREFIX))
        .forEach(e -> Assert.assertEquals(e.getValue(), (datastreams[1].getMetadata().get(e.getKey()))));

    // Explicitly check the additional metadata
    Assert.assertEquals(datastreams[1].getMetadata().get(destMetaKey), destMetaVal);
  }

  // helper method: assert that within a timeout value, the connector are assigned the specific
  // tasks with the specified names.
  private void assertConnectorAssignment(TestHookConnector connector, long timeoutMs, String... datastreamNames)
      throws InterruptedException {

    final long interval = timeoutMs < 100 ? timeoutMs : 100;

    boolean result =
        PollUtils.poll(() -> validateAssignment(connector.getTasks(), datastreamNames), interval, timeoutMs);

    LOG.info(
        String.format("assertConnectorAssignment. Connector: %s, Connector Tasks: %s, ASSERT: %s", connector.getName(),
            connector.getTasks(), result));

    Assert.assertTrue(result);
  }

  private void waitTillAssignmentIsComplete(int totalTasks, long timeoutMs, TestHookConnector... connectors) {
    final long interval = timeoutMs < 100 ? timeoutMs : 100;
    PollUtils.poll(() -> {
      HashSet<DatastreamTask> tasks = new HashSet<>();
      for (TestHookConnector connector : connectors) {
        tasks.addAll(connector.getTasks());
      }
      return tasks.size() == totalTasks;
    }, interval, timeoutMs);
  }

  private boolean validateAssignment(List<DatastreamTask> assignment, String... datastreamNames) {
    if (assignment.size() != datastreamNames.length) {
      LOG.warn("Expected size: " + datastreamNames.length + ", Actual size: " + assignment.size());
      return false;
    }

    Set<String> nameSet = new HashSet<>(Arrays.asList(datastreamNames));
    return assignment.stream()
        .allMatch(t -> nameSet.contains(t.getDatastreams().get(0).getName()) && t.getEventProducer() != null);
  }

  private void deleteLiveInstanceNode(ZkClient zkClient, String cluster, Coordinator instance) {
    String path = KeyBuilder.liveInstance(cluster, instance.getInstanceName());
    zkClient.deleteRecursive(path);
  }

  /**
   * Base class of test Connector implementations
   */
  public class TestHookConnector implements Connector {
    boolean _isStarted = false;
    boolean _allowDatastreamUpdate = true;
    String _connectorType = "TestConnector";
    List<DatastreamTask> _tasks = new ArrayList<>();
    String _instance = "";
    String _name;

    /**
     * Constructor for TestHookConnector
     * @param name Connector name
     * @param connectorType Connector type
     */
    public TestHookConnector(String name, String connectorType) {
      _name = name;
      _connectorType = connectorType;
    }

    /**
     * Constructor for TestHookConnector
     * @param connectorType Connector type
     */
    public TestHookConnector(String connectorType) {
      _connectorType = connectorType;
    }

    public String getName() {
      return _name;
    }

    /**
     * Get all datastream tasks
     */
    public List<DatastreamTask> getTasks() {
      LOG.info(_name + ": getTasks. Instance: " + _instance + ", size: " + _tasks.size() + ", tasks: " + _tasks);
      return _tasks;
    }

    @Override
    public void start(CheckpointProvider checkpointProvider) {
      _isStarted = true;
      LOG.info("Connector " + _name + " started");
    }

    @Override
    public void stop() {
      _isStarted = false;
    }

    @Override
    public void onAssignmentChange(List<DatastreamTask> tasks) {

      LOG.info("START: onAssignmentChange. Name: " + _name + ", ConnectorType: " + _connectorType
          + ",  Number of assignments: " + tasks.size() + ", tasks: " + tasks);

      _tasks = tasks;
      for (DatastreamTask task : tasks) {
        if (task.getEventProducer() == null) {
          Assert.assertNotNull(task.getEventProducer());
        }
      }

      LOG.info("END: onAssignmentChange");
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) {
    }

    @Override
    public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
        throws DatastreamValidationException {
      if (!_allowDatastreamUpdate) {
        throw new DatastreamValidationException("not allowed");
      }
    }

    @Override
    public String toString() {
      return "Connector " + _name + ", StatusId: " + _connectorType + ", Instance: " + _instance;
    }

    @Override
    public List<BrooklinMetricInfo> getMetricInfos() {
      return null;
    }
  }

  /**
   * Test hook connector for mirror maker
   */
  class MMTestHookConnector extends TestHookConnector implements Connector {

    public MMTestHookConnector(String connectorName, String connectorType) {
      super(connectorName, connectorType);
    }

    public DatastreamTask getDatastreamTask(String datastreamName) {
      return _tasks.stream()
          .filter(x -> x.getDatastreams().stream().findFirst().get().getName().equals(datastreamName))
          .findFirst()
          .get();
    }

    @Override
    public void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
        throws DatastreamValidationException {
      if (!_allowDatastreamUpdate) {
        throw new DatastreamValidationException("not allowed");
      }
    }

    @Override
    public boolean isDatastreamUpdateTypeSupported(Datastream datastream, DatastreamConstants.UpdateType updateType) {
      if (DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS == updateType) {
        return true;
      }
      return false;
    }
  }

  /**
   * A {@link Connector} implementation that fails on assignment.
   */
  class BadConnector implements Connector {
    private int assignmentCount;

    public int getAssignmentCount() {
      return assignmentCount;
    }

    @Override
    public void start(CheckpointProvider checkpointProvider) {

    }

    @Override
    public void stop() {

    }

    @Override
    public void onAssignmentChange(List<DatastreamTask> tasks) {
      ++assignmentCount;
      // throw a fake exception to trigger the error handling
      throw new RuntimeException();
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
        throws DatastreamValidationException {
    }

    @Override
    public List<BrooklinMetricInfo> getMetricInfos() {
      return null;
    }
  }

  private class TestSetup {
    public final EmbeddedDatastreamCluster _datastreamKafkaCluster;
    public final Coordinator _coordinator;
    public final DatastreamResources _resource;
    public final TestHookConnector _connector;

    public TestSetup(EmbeddedDatastreamCluster datastreamKafkaCluster, Coordinator coordinator,
        DatastreamResources resource, TestHookConnector connector) {
      _datastreamKafkaCluster = datastreamKafkaCluster;
      _coordinator = coordinator;
      _resource = resource;
      _connector = connector;
    }
  }
}
