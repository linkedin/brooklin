/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.AssignmentToken;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.DatastreamTaskStatus;
import com.linkedin.datastream.server.HostTargetAssignment;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Tests for {@link ZkAdapter}
 */
public class TestZkAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(com.linkedin.datastream.server.zk.TestZkAdapter.class);
  private static final int ZK_WAIT_IN_MS = 500;
  private static final long ZK_DEBOUNCE_TIMER_MS = 1000;

  private final String defaultTransportProviderName = "test";
  private EmbeddedZookeeper _embeddedZookeeper;
  private String _zkConnectionString;

  @BeforeMethod
  public void setup() throws IOException {
    // each embedded ZooKeeper should be on different port
    // so the tests can run in parallel
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
  }

  @AfterMethod
  public void teardown() {
    _embeddedZookeeper.shutdown();
  }

  @Test
  public void testInstanceName() {
    String testCluster = "testInstanceName";

    //
    // create 10 live instances
    //
    ZkAdapter[] adapters = new ZkAdapter[10];
    for (int i = 0; i < 10; i++) {
      adapters[i] = createZkAdapter(testCluster);
      adapters[i].connect();
    }

    //
    // verify the instance names are ending with 00000000 - 00000009
    //
    for (int i = 0; i < 10; i++) {
      String instanceName = adapters[i].getInstanceName();
      Assert.assertTrue(instanceName.contains("0000000" + i));
    }

    //
    // clean up
    //
    for (int i = 0; i < 10; i++) {
      adapters[i].disconnect();
    }
  }

  private ZkAdapter createZkAdapter(String testCluster) {
    return new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName,
        ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT, ZK_DEBOUNCE_TIMER_MS, null);
  }

  @Test
  public void testSmoke() {
    String testCluster = "test_adapter_smoke";

    ZkAdapter adapter1 = createZkAdapter(testCluster);
    adapter1.connect();

    ZkAdapter adapter2 = createZkAdapter(testCluster);
    adapter2.connect();

    // verify ZooKeeper path exists for the two live instance nodes
    ZkClient client = new ZkClient(_zkConnectionString);

    List<String> instances = client.getChildren(KeyBuilder.instances(testCluster));

    Assert.assertEquals(instances.size(), 2);

    adapter1.disconnect();
    adapter2.disconnect();

    client.close();
  }

  @Test
  public void testLeaderElection() {
    String testCluster = "test_adapter_leader";

    //
    // start two ZkAdapters, which is corresponding to two Coordinator instances
    //
    ZkAdapter adapter1 = new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName,
        1000, 15000, ZK_DEBOUNCE_TIMER_MS, null);
    adapter1.connect();

    ZkAdapter adapter2 = new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName,
        1000, 15000, ZK_DEBOUNCE_TIMER_MS, null);
    adapter2.connect();

    //
    // verify the first one started is the leader, and the second one is a follower
    //
    Assert.assertTrue(adapter1.isLeader());
    Assert.assertFalse(adapter2.isLeader());

    //
    // disconnect the first adapter to simulate it going offline
    //
    adapter1.disconnect();

    //
    // wait for leadership election to happen
    // adapter2 should now be the new leader
    //
    Assert.assertTrue(PollUtils.poll(adapter2::isLeader, 100, ZK_WAIT_IN_MS));

    //
    // adapter2 goes offline, but new instance adapter2 goes online
    //
    adapter2.disconnect();
    // now a new client goes online
    ZkAdapter adapter3 = new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName,
        1000, 15000, ZK_DEBOUNCE_TIMER_MS, null);
    adapter3.connect();

    //
    // verify that the adapter3 is the current leader
    //
    Assert.assertTrue(PollUtils.poll(adapter3::isLeader, 100, ZK_WAIT_IN_MS));
  }

  @Test
  public void testStressLeaderElection() {
    String testCluster = "test_leader_election_stress";

    //
    // start 50 adapters, simulating 50 live instances
    //
    int concurrencyLevel = 50;
    ZkAdapter[] adapters = new ZkAdapter[concurrencyLevel];

    for (int i = 0; i < concurrencyLevel; i++) {
      adapters[i] = createZkAdapter(testCluster);
      adapters[i].connect();
    }

    //
    // verify the first adapter adapters[0] is the leader
    //
    Assert.assertTrue(adapters[0].isLeader());
    //
    // verify the second adapter adapters[1] is the follower
    //
    Assert.assertFalse(adapters[1].isLeader());
    //
    // stop the second adapter adapters[1] to create a hole
    //
    adapters[1].disconnect();
    //
    // verify leader not changed
    //
    Assert.assertTrue(adapters[0].isLeader());
    //
    // stop first half adapters adapters[0..concurrencyLevel/2]
    //
    for (int i = 0; i < concurrencyLevel / 2; i++) {
      if (i != 1) {
        adapters[i].disconnect();
      }
    }
    //
    // new leader should be the next inline, adapters[concurrencyLevel/2]
    //
    Assert.assertTrue(PollUtils.poll(() -> adapters[concurrencyLevel / 2].isLeader(), 100, ZK_WAIT_IN_MS));

    //
    // clean up
    //
    for (int i = 0; i < concurrencyLevel; i++) {
      adapters[i] = createZkAdapter(testCluster);
      adapters[i].disconnect();
    }
  }

  private void validateConnectorTask(String cluster, String connectorType, String task, String instance, ZkClient zkClient) {
    List<String> connectorAssignment = zkClient.getChildren(KeyBuilder.connectorTask(cluster, connectorType, task));
    Assert.assertEquals(connectorAssignment.size(), 2); // state + config
    Assert.assertTrue(connectorAssignment.contains("state"));
    Assert.assertTrue(connectorAssignment.contains("config"));
    Assert.assertEquals(instance, zkClient.ensureReadData(KeyBuilder.connectorTask(cluster, connectorType, task)));
  }

  // When Coordinator leader writes the assignment to a specific instance, the change is indeed
  // persisted in Zookeper
  @Test
  public void testUpdateInstanceAssignment() throws Exception {
    String testCluster = "testUpdateInstanceAssignment";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter = createZkAdapter(testCluster);
    adapter.connect();

    // Create all the Datastreams to be referenced by the tasks
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType, "task1", "task2", "task3");

    List<DatastreamTask> tasks = new ArrayList<>();

    //
    // simulate assigning one task [task1] to the connector
    //
    DatastreamTaskImpl task1 = new DatastreamTaskImpl();
    task1.setTaskPrefix("task1");
    task1.setConnectorType(connectorType);
    tasks.add(task1);
    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);
    //
    // verify that there are 1 znode under the Zookeper /{cluster}/instances/{instance}/
    //
    List<String> assignment =
        zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter.getInstanceName()));
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 1, 100, 5000, assignment));
    Assert.assertEquals(assignment.get(0), "task1");
    validateConnectorTask(testCluster, connectorType, "task1", adapter.getInstanceName(), zkClient);

    //
    // simulate assigning two tasks [task1, task2] to the same instance
    //
    DatastreamTaskImpl task2 = new DatastreamTaskImpl();
    task2.setTaskPrefix("task2");
    task2.setConnectorType(connectorType);
    tasks.add(task2);
    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);
    //
    // verify that there are 2 znodes under the Zookeper path /{cluster}/instances/{instance}
    //
    assignment = zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter.getInstanceName()));
    Collections.sort(assignment);
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 2, 100, 5000, assignment));
    Assert.assertEquals(assignment.get(0), "task1");
    Assert.assertEquals(assignment.get(1), "task2");
    validateConnectorTask(testCluster, connectorType, "task1", adapter.getInstanceName(), zkClient);
    validateConnectorTask(testCluster, connectorType, "task2", adapter.getInstanceName(), zkClient);

    //
    // simulate removing task2 and adding task3 to the assignment, now the tasks are [task1, task3]
    //
    DatastreamTaskImpl task3 = new DatastreamTaskImpl();
    task3.setTaskPrefix("task3");
    task3.setConnectorType(connectorType);
    tasks.add(task3);
    tasks.remove(task2);
    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);
    //
    // verify that there are still 2 znodes under Zookeper path /{cluster}/instances/{instance}
    //
    assignment = zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter.getInstanceName()));
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 2, 100, 5000, assignment));
    Collections.sort(assignment);
    Assert.assertEquals(assignment.get(0), "task1");
    Assert.assertEquals(assignment.get(1), "task3");
    validateConnectorTask(testCluster, connectorType, "task1", adapter.getInstanceName(), zkClient);
    validateConnectorTask(testCluster, connectorType, "task3", adapter.getInstanceName(), zkClient);

    //
    // cleanup
    //
    zkClient.close();
  }

  // When Coordinator leader writes the assignment to a specific instance, the change is indeed
  // persisted in Zookeper
  @Test
  public void testUpdateAllAssignments() throws Exception {
    String testCluster = "testUpdateAllInstanceAssignment";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter1 = createZkAdapter(testCluster);
    ZkAdapter adapter2 = createZkAdapter(testCluster);
    adapter1.connect();
    adapter2.connect();

    // Create all the Datastreams to be referenced by the tasks
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType, "task1", "task2", "task3");

    //
    // simulate assigning:
    //   to instance1: [task1]
    //   to instance2: [task2, task3]
    //
    DatastreamTaskImpl task1 = new DatastreamTaskImpl();
    task1.setTaskPrefix("task1");
    task1.setConnectorType(connectorType);

    DatastreamTaskImpl task2 = new DatastreamTaskImpl();
    task2.setTaskPrefix("task2");
    task2.setConnectorType(connectorType);

    DatastreamTaskImpl task3 = new DatastreamTaskImpl();
    task3.setTaskPrefix("task3");
    task3.setConnectorType(connectorType);

    Map<String, List<DatastreamTask>> assignmentsByInstance = new HashMap<>();
    assignmentsByInstance.put(adapter1.getInstanceName(), Collections.singletonList(task1));
    assignmentsByInstance.put(adapter2.getInstanceName(), Arrays.asList(task2, task3));

    // Single call to update all assignments.
    adapter1.updateAllAssignments(assignmentsByInstance);

    //
    // verify that there are 1 znode under the Zookeper /{cluster}/instances/{instance1}/
    //
    List<String> assignment1 =
        zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter1.getInstanceName()));
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 1, 100, 5000, assignment1));
    Assert.assertEquals(assignment1.get(0), "task1");
    validateConnectorTask(testCluster, connectorType, "task1", adapter1.getInstanceName(), zkClient);

    //
    // verify that there are 2 znode under the Zookeper /{cluster}/instances/{instance2}/
    //
    List<String> assignment2 =
        zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter2.getInstanceName()));
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 2, 100, 5000, assignment2));
    Assert.assertEquals(assignment2.get(0), "task2");
    Assert.assertEquals(assignment2.get(1), "task3");

    //
    // cleanup
    //
    zkClient.close();
  }

  @Test
  public void testGetPartitionMovement() throws Exception {
    String testCluster = "testGetPartitionMovement";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter1 = createZkAdapter(testCluster);

    Datastream[] datastreams = DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType, "datastream1");
    DatastreamGroup datastreamGroup = new DatastreamGroup(Arrays.asList(datastreams));

    adapter1.connect();
    PollUtils.poll(adapter1::isLeader, 50, 5000);
    List<String> instances = adapter1.getLiveInstances();
    String hostName = instances.get(0).substring(0, instances.get(0).lastIndexOf('-'));

    long current = System.currentTimeMillis();
    String path = KeyBuilder.getTargetAssignmentPath(testCluster, connectorType, datastreamGroup.getName());
    HostTargetAssignment targetAssignment = new HostTargetAssignment(ImmutableList.of("t-0", "t-1"), hostName);
    zkClient.ensurePath(path);
    if (zkClient.exists(path)) {
      String json = targetAssignment.toJson();
      zkClient.ensurePath(path + '/' + current);
      zkClient.writeData(path + '/' + current, json);
    }

    long current2 = System.currentTimeMillis();

    Map<String, Set<String>> newAssignment = adapter1.getPartitionMovement(datastreamGroup.getConnectorName(),
        datastreamGroup.getName(), current2);
    Assert.assertEquals(newAssignment.get(instances.get(0)), ImmutableSet.of("t-0", "t-1"));
    adapter1.disconnect();
    zkClient.close();
  }

  @Test
  public void testMultiplePartitionMovement() throws Exception {
    String testCluster = "testGetPartitionMovement";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter1 = spy(createZkAdapter(testCluster));
    ZkAdapter adapter2 = createZkAdapter(testCluster);

    Datastream[] datastreams = DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType, "datastream1");
    DatastreamGroup datastreamGroup = new DatastreamGroup(Arrays.asList(datastreams));

    adapter1.connect();
    adapter2.connect();
    PollUtils.poll(adapter1::isLeader, 50, 5000);
    PollUtils.poll(() -> !adapter2.isLeader(), 50, 5000);

    List<String> hostnames = ImmutableList.of("host000-000", "host001-000");
    doReturn(hostnames).when(adapter1).getLiveInstances();
    List<String> instances = adapter1.getLiveInstances();
    String hostName1 = instances.get(0).substring(0, instances.get(0).lastIndexOf('-'));
    String hostName2 = instances.get(1).substring(0, instances.get(0).lastIndexOf('-'));

    String path = KeyBuilder.getTargetAssignmentPath(testCluster, connectorType, datastreamGroup.getName());
    List<HostTargetAssignment> assignments = ImmutableList.of(new HostTargetAssignment(ImmutableList.of("t-0", "t-1"), hostName1),
        new HostTargetAssignment(ImmutableList.of("t-1", "t-3"), hostName2),
        new HostTargetAssignment(ImmutableList.of("t-2", "t-4"), hostName1));

    zkClient.ensurePath(path);
    if (zkClient.exists(path)) {
      for (HostTargetAssignment assignment : assignments) {
        long current1 = System.currentTimeMillis();
        zkClient.ensurePath(path + '/' + current1);
        zkClient.writeData(path + '/' + current1,  assignment.toJson());
        Thread.sleep(100);
      }
    }

    long current2 = System.currentTimeMillis();

    Map<String, Set<String>> newAssignment = adapter1.getPartitionMovement(datastreamGroup.getConnectorName(),
        datastreamGroup.getName(), current2);
    Assert.assertEquals(newAssignment.get(instances.get(0)), ImmutableSet.of("t-0", "t-2", "t-4"));
    Assert.assertEquals(newAssignment.get(instances.get(1)), ImmutableSet.of("t-1", "t-3"));
    adapter1.disconnect();
    zkClient.close();
  }


  @Test
  // CHECKSTYLE:OFF
  public void testInstanceAssignmentWithPartitions() {
    String testCluster = "testInstanceAssignmentWithPartitions";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter = createZkAdapter(testCluster);
    adapter.connect();

    List<DatastreamTask> tasks = new ArrayList<>();

    //
    // simulate assigning one task [task1] to the connector. task1 has 4 partitions
    //
    DatastreamTaskImpl task1_0 = new DatastreamTaskImpl();
    task1_0.setTaskPrefix("task1");
    task1_0.setId("0");
    task1_0.setConnectorType(connectorType);
    tasks.add(task1_0);

    DatastreamTaskImpl task1_1 = new DatastreamTaskImpl();
    task1_1.setId("1");
    task1_1.setTaskPrefix("task1");
    task1_1.setConnectorType(connectorType);
    tasks.add(task1_1);

    DatastreamTaskImpl task1_2 = new DatastreamTaskImpl();
    task1_2.setTaskPrefix("task1");
    task1_2.setId("2");
    task1_2.setConnectorType(connectorType);
    tasks.add(task1_2);

    DatastreamTaskImpl task1_3 = new DatastreamTaskImpl();
    task1_3.setTaskPrefix("task1");
    task1_3.setId("3");
    task1_3.setConnectorType(connectorType);
    tasks.add(task1_3);

    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);

    //
    // verify there are 4 znodes under Zookeper path /{cluster}/instances/{instance}
    //
    List<String> assignment =
        zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter.getInstanceName()));
    Collections.sort(assignment);
    Assert.assertEquals(assignment.size(), 4);
    Assert.assertEquals(assignment.get(0), "task1_0");
    Assert.assertEquals(assignment.get(1), "task1_1");
    Assert.assertEquals(assignment.get(2), "task1_2");
    Assert.assertEquals(assignment.get(3), "task1_3");

    //
    // cleanup
    //
    zkClient.close();
  }
  // CHECKSTYLE:ON

  interface TestFunction {
    void execute() throws Throwable;
  }

  private boolean expectException(TestFunction func, boolean exception) {
    boolean exceptionSeen = false;
    try {
      func.execute();
    } catch (Throwable t) {
      exceptionSeen = true;
    }
    return exception == exceptionSeen;
  }

  @Test
  public void testTaskAcquireRelease() {
    String testCluster = "testTaskAcquireRelease";
    String connectorType = "connectorType";
    Duration timeout = Duration.ofSeconds(1);

    ZkAdapter adapter1 = createZkAdapter(testCluster);
    adapter1.connect();

    DatastreamTaskImpl task = new DatastreamTaskImpl();
    task.setId("3");
    task.setConnectorType(connectorType);
    task.setZkAdapter(adapter1);

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(task);
    updateInstanceAssignment(adapter1, adapter1.getInstanceName(), tasks);

    // First acquire should succeed
    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));

    // Acquire twice should succeed
    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));

    ZkAdapter adapter2 = createZkAdapter(testCluster);
    adapter2.connect();

    // Acquire from instance2 should fail
    task.setZkAdapter(adapter2);
    Assert.assertTrue(expectException(() -> task.acquire(Duration.ofMillis(100)), true));

    // Release the task from instance1
    task.setZkAdapter(adapter1);
    task.release();

    // Now acquire from instance2 should succeed
    task.setZkAdapter(adapter2);
    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));
  }

  /**
   * Test task acquire when the current owner has shutdown uncleanly,
   * such that the owner didn't get the chance to release the task.
   */
  @Test
  public void testTaskAcquireReleaseOwnerUncleanShutdown() {
    String testCluster = "testTaskAcquireReleaseOwnerUncleanShutdown";
    String connectorType = "connectorType";
    Duration timeout = Duration.ofSeconds(10);

    ZkAdapter adapter1 = createZkAdapter(testCluster);
    adapter1.connect();

    DatastreamTaskImpl task = new DatastreamTaskImpl();
    task.setId("3");
    task.setConnectorType(connectorType);
    task.setZkAdapter(adapter1);

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(task);
    updateInstanceAssignment(adapter1, adapter1.getInstanceName(), tasks);

    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));

    ZkAdapter adapter2 = createZkAdapter(testCluster);
    adapter2.connect();

    LOG.info("Acquire from instance2 should fail");
    task.setZkAdapter(adapter2);
    Assert.assertTrue(expectException(() -> task.acquire(Duration.ofMillis(100)), true));

    LOG.info("Disconnecting instance1");
    adapter1.disconnect();

    LOG.info("instance2 should be able to acquire after instance1's disconnection");
    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));
  }

  /**
   * Test task acquire when the current owner was bounced uncleanly,
   * such that the owner didn't get the chance to release the task.
   */
  @Test
  public void testTaskAcquireReleaseOwnerUncleanBounce() {
    String testCluster = "testTaskAcquireReleaseOwnerUncleanBounce";
    String connectorType = "connectorType";
    Duration timeout = Duration.ofSeconds(15);

    ZkAdapter adapter1 = createZkAdapter(testCluster);
    adapter1.connect();

    DatastreamTaskImpl task = new DatastreamTaskImpl();
    task.setId("3");
    task.setConnectorType(connectorType);
    task.setZkAdapter(adapter1);

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(task);
    updateInstanceAssignment(adapter1, adapter1.getInstanceName(), tasks);

    LOG.info("Acquire from instance1 should succeed");
    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));

    ZkAdapter adapter2 = createZkAdapter(testCluster);
    adapter2.connect();

    LOG.info("Acquire from instance2 should fail");
    task.setZkAdapter(adapter2);
    Assert.assertTrue(expectException(() -> task.acquire(Duration.ofMillis(100)), true));

    LOG.info("Disconnecting instance1");
    String instanceName1 = adapter1.getInstanceName();
    adapter1.disconnect();

    LOG.info("Waiting up to 5 seconds for instance2 to become leader");
    PollUtils.poll(adapter2::isLeader, 50, 5000);

    LOG.info("Wait up to 5 seconds for the ephemeral node to be gone");
    PollUtils.poll(() -> !adapter2.getLiveInstances().contains(instanceName1), 50, 5000);

    LOG.info("Reconnecting instance1 should get a new instanceName");
    adapter1.connect();
    Assert.assertNotEquals(instanceName1, adapter1.getInstanceName());

    LOG.info("instance2 should be able to acquire since old instance1 is dead");
    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));

    LOG.info("Acquire from the new instance1 should fail");
    task.setZkAdapter(adapter1);
    Assert.assertTrue(expectException(() -> task.acquire(Duration.ofMillis(100)), true));
  }


  /**
   * Test task acquire when there are dependencies
   */
  @Test
  public void testTaskAcquireWithDependencies() throws InterruptedException {
    String testCluster = "testTaskAcquireReleaseOwnerUncleanBounce";
    String connectorType = "connectorType";

    ZkAdapter adapter1 = createZkAdapter(testCluster);
    adapter1.connect();

    DatastreamTaskImpl task1 = new DatastreamTaskImpl();
    task1.setId("3");
    task1.setConnectorType(connectorType);
    task1.setZkAdapter(adapter1);
    task1.setPartitionsV2(ImmutableList.of("partition1"));

    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(task1);
    updateInstanceAssignment(adapter1, adapter1.getInstanceName(), tasks);

    LOG.info("Acquire from instance1 should succeed");
    Assert.assertTrue(expectException(() -> task1.acquire(Duration.ofMillis(100)), false));

    //The task2 cannot be acquired as the dependencies are not released
    DatastreamTaskImpl task2 = new DatastreamTaskImpl(task1, new ArrayList<>());
    Assert.assertTrue(expectException(() -> task2.acquire(Duration.ofMillis(100)), true));
    Assert.assertTrue(expectException(() -> task2.acquire(Duration.ofMillis(ZK_DEBOUNCE_TIMER_MS)), true));
    Thread.sleep(2000);
    //Verify the task2 can be locked after task1 is released
    Thread acquireThread = new Thread(() -> task2.acquire(Duration.ofSeconds(3)));
    Thread releaseThread = new Thread(task1::release);

    acquireThread.start();
    releaseThread.start();

    Assert.assertTrue(PollUtils.poll(task2::isLocked, 100, 5000));
  }

  private ZkClientInterceptingAdapter createInterceptingZkAdapter(String testCluster) {
    return createInterceptingZkAdapter(testCluster, ZkClient.DEFAULT_SESSION_TIMEOUT, (int) (ZK_DEBOUNCE_TIMER_MS * 2));
  }

  private ZkClientInterceptingAdapter createInterceptingZkAdapter(String testCluster, int sessionTimeoutMs, long debounceTimerMs) {
    return spy(new ZkClientInterceptingAdapter(_zkConnectionString, testCluster, defaultTransportProviderName,
        sessionTimeoutMs, ZkClient.DEFAULT_CONNECTION_TIMEOUT,  debounceTimerMs, null));
  }

  private static class ZkClientInterceptingAdapter extends ZkAdapter {
    private ZkClient _zkClient;
    private long _sleepMs;

    public ZkClientInterceptingAdapter(String zkConnectionString, String testCluster, String defaultTransportProviderName,
        int defaultSessionTimeoutMs, int defaultConnectionTimeoutMs, long debounceTimerMs, ZkAdapterListener listener) {
      super(zkConnectionString, testCluster, defaultTransportProviderName, defaultSessionTimeoutMs,
          defaultConnectionTimeoutMs, debounceTimerMs, listener);
      _sleepMs = defaultSessionTimeoutMs;
    }

    @Override
    ZkClient createZkClient() {
      _zkClient = super.createZkClient();
      return _zkClient;
    }

    public ZkClient getZkClient() {
      return _zkClient;
    }
  }

  @Test
  public void testZookeeperSessionExpiryDontReinitNewSession() throws InterruptedException {
    testZookeeperSessionExpiry(false);
  }
  @Test
  public void testZookeeperSessionExpiryReinitNewSession() throws InterruptedException {
    testZookeeperSessionExpiry(true);
  }

  private void testZookeeperSessionExpiry(boolean reinitNewSession) throws InterruptedException {
    String testCluster = "testDeleteTaskWithPrefix";
    String connectorType = "connectorType";
    Duration timeout = Duration.ofMinutes(1);

    ZkClientInterceptingAdapter adapter = createInterceptingZkAdapter(testCluster, 5000, ZK_DEBOUNCE_TIMER_MS);
    adapter.connect(reinitNewSession);

    ZkClientInterceptingAdapter adapter2 = createInterceptingZkAdapter(testCluster, 5000, ZK_DEBOUNCE_TIMER_MS);
    adapter2.connect(reinitNewSession);

    DatastreamTaskImpl task = new DatastreamTaskImpl();
    task.setId("3");
    task.setConnectorType(connectorType);
    task.setZkAdapter(adapter);

    List<DatastreamTask> tasks = Collections.singletonList(task);
    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);

    LOG.info("Acquire from instance1 should succeed");
    Assert.assertTrue(expectException(() -> task.acquire(timeout), false));

    Assert.assertTrue(adapter.isLeader());
    Assert.assertFalse(adapter2.isLeader());
    verifyZkListenersOfLeader(adapter);
    verifyZkListenersOfFollower(adapter2);

    simulateSessionExpiration(adapter);

    Thread.sleep(5000);
    verify(adapter, times(1)).onSessionExpired();
    if (!reinitNewSession) {
      verifyZkListenersAfterDisconnect(adapter);
    } else {
      verifyZkListenersAfterExpiredSession(adapter);
    }
    Assert.assertFalse(adapter.isLeader());
    Assert.assertTrue(PollUtils.poll(adapter2::isLeader, 100, ZK_WAIT_IN_MS));
    verifyZkListenersOfLeader(adapter2);

    Assert.assertTrue(PollUtils.poll(adapter2::isLeader, 100, ZK_WAIT_IN_MS));
    Assert.assertTrue(adapter2.isLeader());

    //This connect is called from the coordinator code, calling it explicitly here for testing.
    if (reinitNewSession) {
      verify(adapter, times(1)).onNewSession();
      Assert.assertFalse(adapter.isLeader());
      adapter.connect();
      verifyZkListenersOfFollower(adapter);
    }

    adapter2.disconnect();
    verifyZkListenersAfterDisconnect(adapter2);

    if (reinitNewSession) {
      Assert.assertTrue(PollUtils.poll(adapter::isLeader, 100, ZK_WAIT_IN_MS));
      verifyZkListenersOfLeader(adapter);

      adapter.disconnect();
      verifyZkListenersAfterDisconnect(adapter);
    }
  }

  private void verifyZkListenersAfterDisconnect(ZkClientInterceptingAdapter adapter) {
    Assert.assertNull(adapter.getLeaderElectionListener());
    Assert.assertNull(adapter.getAssignmentListProvider());
    Assert.assertNull(adapter.getStateChangeListener());
    Assert.assertNull(adapter.getLiveInstancesProvider());
    Assert.assertNull(adapter.getDatastreamList());
    Assert.assertNull(adapter.getTargetAssignmentProvider());
  }

  private void verifyZkListenersAfterExpiredSession(ZkClientInterceptingAdapter adapter) {
    Assert.assertNotNull(adapter.getLeaderElectionListener());
    Assert.assertNull(adapter.getAssignmentListProvider());
    Assert.assertNotNull(adapter.getStateChangeListener());
    Assert.assertNull(adapter.getLiveInstancesProvider());
    Assert.assertNull(adapter.getDatastreamList());
    Assert.assertNull(adapter.getTargetAssignmentProvider());
  }

  private void verifyZkListenersOfFollower(ZkClientInterceptingAdapter adapter2) {
    Assert.assertNotNull(adapter2.getLeaderElectionListener());
    Assert.assertNotNull(adapter2.getAssignmentListProvider());
    Assert.assertNotNull(adapter2.getStateChangeListener());
    Assert.assertNull(adapter2.getLiveInstancesProvider());
    Assert.assertNull(adapter2.getDatastreamList());
    Assert.assertNull(adapter2.getTargetAssignmentProvider());
  }

  private void verifyZkListenersOfLeader(ZkClientInterceptingAdapter adapter) {
    Assert.assertNotNull(adapter.getLeaderElectionListener());
    Assert.assertNotNull(adapter.getAssignmentListProvider());
    Assert.assertNotNull(adapter.getStateChangeListener());
    Assert.assertNotNull(adapter.getLiveInstancesProvider());
    Assert.assertNotNull(adapter.getDatastreamList());
    Assert.assertNotNull(adapter.getTargetAssignmentProvider());
  }

  @Test
  public void testZookeeperLockAcquire() throws InterruptedException {
    String testCluster = "testLockAcquire";
    String connectorType = "connectorType";
    //
    // start two ZkAdapters, which is corresponding to two Coordinator instances
    //
    ZkClientInterceptingAdapter adapter1 = createInterceptingZkAdapter(testCluster, 5000, ZK_DEBOUNCE_TIMER_MS * 10);
    adapter1.connect();

    DatastreamTaskImpl task = new DatastreamTaskImpl();
    task.setId("3");
    task.setConnectorType(connectorType);
    task.setZkAdapter(adapter1);

    List<DatastreamTask> tasks = Collections.singletonList(task);
    updateInstanceAssignment(adapter1, adapter1.getInstanceName(), tasks);

    LOG.info("Acquire from instance1 should succeed");
    Duration timeout = Duration.ofSeconds(3);
    Assert.assertTrue(expectException(() -> adapter1.acquireTask(task, timeout), false));
    String owner = adapter1.getZkClient().readData(KeyBuilder.datastreamTaskLock(testCluster, task.getConnectorType(),
        task.getTaskPrefix(), task.getDatastreamTaskName()));

    ZkClientInterceptingAdapter adapter2 = createInterceptingZkAdapter(testCluster, 5000, ZK_DEBOUNCE_TIMER_MS * 10);
    adapter2.connect();
    simulateSessionExpiration(adapter1);

    Thread.sleep(1000);
    Assert.assertTrue(expectException(() -> adapter1._zkClient.waitUntilConnected(5, TimeUnit.SECONDS), false));

    // adapter2 not able to acquire lock
    Assert.assertTrue(adapter2.getZkClient().exists(KeyBuilder.datastreamTaskLock(testCluster, task.getConnectorType(),
        task.getTaskPrefix(), task.getDatastreamTaskName())));
    Assert.assertTrue(expectException(() -> adapter2.acquireTask(task, timeout), true));

    Assert.assertTrue(adapter2.getZkClient().exists(KeyBuilder.datastreamTaskLock(testCluster, task.getConnectorType(),
        task.getTaskPrefix(), task.getDatastreamTaskName())));
    String owner2 = adapter2.getZkClient().readData(KeyBuilder.datastreamTaskLock(testCluster, task.getConnectorType(),
        task.getTaskPrefix(), task.getDatastreamTaskName()));
    Assert.assertEquals(owner, owner2);

    // adapter 2 able to acquire lock
    Assert.assertTrue(expectException(() -> adapter2.acquireTask(task, Duration.ofSeconds(15)), false));

    Assert.assertTrue(adapter2.getZkClient().exists(KeyBuilder.datastreamTaskLock(testCluster, task.getConnectorType(),
        task.getTaskPrefix(), task.getDatastreamTaskName())));
    owner2 = adapter2.getZkClient().readData(KeyBuilder.datastreamTaskLock(testCluster, task.getConnectorType(),
        task.getTaskPrefix(), task.getDatastreamTaskName()));
    Assert.assertNotEquals(owner, owner2);
  }

  private void simulateSessionExpiration(ZkClientInterceptingAdapter adapter) {
    long sessionId = adapter.getSessionId();
    LOG.info("Closing/expiring session: " + sessionId);
    _embeddedZookeeper.closeSession(sessionId);
  }

  @Test
  public void testDeleteTasksWithPrefix() throws InterruptedException {
    String testCluster = "testDeleteTaskWithPrefix";
    String connectorType = "connectorType";

    ZkClientInterceptingAdapter adapter = createInterceptingZkAdapter(testCluster);
    adapter.connect();

    adapter._zkClient.ensurePath(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(adapter.cleanUpOrphanConnectorTaskLocks(false), 0);

    // lock node
    DatastreamTaskImpl lockTask = new DatastreamTaskImpl();
    lockTask.setId("task" + "lock");
    lockTask.setTaskPrefix("taskPrefix" + "lock");
    lockTask.setConnectorType(connectorType);
    lockTask.setZkAdapter(adapter);

    List<DatastreamTask> tasks = new ArrayList<>();
    // Create some nodes
    for (int i = 0; i < 10; i++) {
      DatastreamTaskImpl dsTask = new DatastreamTaskImpl();
      dsTask.setId("task" + i);
      String taskPrefix = "taskPrefix" + i;
      dsTask.setTaskPrefix(taskPrefix);
      dsTask.setConnectorType(connectorType);
      dsTask.setZkAdapter(adapter);
      tasks.add(dsTask);
      adapter.acquireTask(dsTask, Duration.ofSeconds(2));
    }
    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);
    adapter.acquireTask(lockTask, Duration.ofSeconds(2));

    ZkClient zkClient = spy(adapter.getZkClient());

    // Delete a few nodes
    for (int j = 0; j < 8; j++) {
      adapter.deleteTasksWithPrefix(connectorType, "taskPrefix" + j);
    }

    // Verify delete was successful with no calls done to getChildren
    // Not the most ideal way to test the issue of not being able to delete when the top level zk node is full,
    // but creating EmbeddedZK with smaller jute.maxbuffer size to actually testing filling a directory to
    // max requires setting system property which will interfere with any other parallel test using EmbeddedZk.
    verify(zkClient, never()).getChildren(any());
    verify(zkClient, never()).getChildren(any(), anyBoolean());

    List<String> leftOverTasks = zkClient.getChildren(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(leftOverTasks.size(), 3);

    adapter.cleanUpOrphanConnectorTasks(false);

    leftOverTasks = zkClient.getChildren(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(leftOverTasks.size(), 3);

    // Verify orphan locks, lockTask is the only orphan task.
    // expected 1 * 2, because its the only node, it needs to delete the prefix node as well.
    Assert.assertEquals(adapter.cleanUpOrphanConnectorTaskLocks(false), 1 * 2);
    Map<String, Set<String>> taskLocks = adapter.getAllConnectorTaskLocks(connectorType);
    Assert.assertEquals(taskLocks.size(), 11);
    Assert.assertEquals(taskLocks.get("taskPrefix0").size(), 1);
    Assert.assertEquals(taskLocks.get("taskPrefixlock").size(), 1);

    leftOverTasks = zkClient.getChildren(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(leftOverTasks.size(), 3);

    updateInstanceAssignment(adapter, adapter.getInstanceName(), Collections.emptyList());

    leftOverTasks = zkClient.getChildren(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(leftOverTasks.size(), 3);

    adapter.cleanUpOrphanConnectorTasks(false);

    leftOverTasks = zkClient.getChildren(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(leftOverTasks.size(), 3);

    adapter.cleanUpOrphanConnectorTasks(true);
    Assert.assertEquals(adapter.cleanUpOrphanConnectorTaskLocks(true), 11 * 2);
    taskLocks = adapter.getAllConnectorTaskLocks(connectorType);
    Assert.assertEquals(taskLocks.size(), 11);
    Assert.assertEquals(taskLocks.get("taskPrefix0").size(), 1);
    Assert.assertEquals(taskLocks.get("taskPrefixlock").size(), 1);

    // Thread sleep to wait for debounce timer orphan lock cleanup.
    Thread.sleep(5000);
    taskLocks = adapter.getAllConnectorTaskLocks(connectorType);
    Assert.assertEquals(taskLocks.size(), 0);

    leftOverTasks = zkClient.getChildren(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(leftOverTasks.size(), 1);
    Assert.assertEquals(leftOverTasks.get(0), KeyBuilder.DATASTREAM_TASK_LOCK_ROOT_NAME);

    // lock root node does not get deleted, once created.
    adapter.releaseTask(lockTask);
    leftOverTasks = zkClient.getChildren(KeyBuilder.connector(testCluster, connectorType));
    Assert.assertEquals(leftOverTasks.size(), 1);

    adapter.disconnect();
  }

  @Test
  public void testUpdateAssignmentDeleteUnusedTasks() {
    String testCluster = "testUpdateAssignmentDeleteUnusedTasks";
    String connectorType = "connectorType";
    int totalTasks = 10;

    ZkClientInterceptingAdapter adapter = createInterceptingZkAdapter(testCluster);
    adapter.connect();

    List<DatastreamTask> tasks1 = new ArrayList<>();
    List<DatastreamTask> newTasks1 = new ArrayList<>();
    List<DatastreamTask> tasks2 = new ArrayList<>();

    // Create some nodes
    for (int i = 0; i < totalTasks; i++) {
      DatastreamTaskImpl dsTask = new DatastreamTaskImpl();
      dsTask.setId("task1" + i);
      String taskPrefix = "taskPrefix1" + i;
      dsTask.setTaskPrefix(taskPrefix);
      dsTask.setConnectorType(connectorType);
      dsTask.setZkAdapter(adapter);

      if (i % 2 == 0) {
        tasks1.add(dsTask);
        newTasks1.add(dsTask);
      } else {
        tasks2.add(dsTask);
      }
    }

    Map<String, List<DatastreamTask>> oldAssignments = new HashMap<>();
    oldAssignments.put(adapter.getInstanceName(), tasks1);
    oldAssignments.put("deadInstance-999", tasks2);
    adapter.updateAllAssignments(oldAssignments);

    Map<String, Set<DatastreamTask>> currentAssignments = adapter.getAllAssignedDatastreamTasks();
    Assert.assertEquals(oldAssignments.keySet(), currentAssignments.keySet());
    currentAssignments.forEach((k, v) -> Assert.assertEquals(v, new HashSet<>(oldAssignments.get(k))));

    newTasks1.remove(0);
    Map<String, List<DatastreamTask>> newAssignments = new HashMap<>();
    newAssignments.put(adapter.getInstanceName(), newTasks1);

    adapter.cleanUpDeadInstanceDataAndOtherUnusedTasks(currentAssignments, newAssignments,
        new ArrayList<>(newAssignments.keySet()));

    List<String> leftOverTasks = adapter.getZkClient().getChildren(KeyBuilder.connector(testCluster, connectorType));
    // Verify that unused connector tasks got cleaned up.
    Assert.assertEquals(leftOverTasks.size(), newTasks1.size());

    // Verify that dead instance got cleaned up.
    Assert.assertTrue(adapter.getZkClient().exists(KeyBuilder.instance(testCluster, adapter.getInstanceName())));
    Assert.assertFalse(adapter.getZkClient().exists(KeyBuilder.instance(testCluster, "deadInstance-999")));

    adapter.disconnect();
  }

  @Test
  public void testUpdateAllAssignmentAndIssueTokens() throws Exception {
    String testCluster = "testUpdateAllAssignmentAndIssueTokens";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter = createZkAdapter(testCluster);
    adapter.connect();

    // assigning 2 tasks to the cluster
    Datastream[] datastreams1 = DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType,
        "datastream1");
    DatastreamGroup datastreamGroup1 = new DatastreamGroup(Arrays.asList(datastreams1));

    Datastream[] datastreams2 = DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType,
        "datastream2");
    DatastreamGroup datastreamGroup2 = new DatastreamGroup(Arrays.asList(datastreams2));

    DatastreamTaskImpl task1 = new DatastreamTaskImpl();
    task1.setTaskPrefix(datastreamGroup1.getTaskPrefix());
    task1.setConnectorType(connectorType);

    DatastreamTaskImpl task2 = new DatastreamTaskImpl();
    task2.setTaskPrefix(datastreamGroup2.getTaskPrefix());
    task2.setConnectorType(connectorType);

    Map<String, List<DatastreamTask>> oldAassignment = new HashMap<>();
    oldAassignment.put("instance1", Collections.singletonList(task1));
    oldAassignment.put("instance2", Collections.singletonList(task2));

    adapter.updateAllAssignments(oldAassignment);

    // simulating a stopping datastream which has a task assigned to instance2
    List<DatastreamGroup> stoppingDatastreamGroups = Collections.singletonList(datastreamGroup2);
    Map<String, List<DatastreamTask>> newAssignment = new HashMap<>();
    newAssignment.put("instance1", Collections.singletonList(task1));
    adapter.updateAllAssignmentsAndIssueTokens(newAssignment, stoppingDatastreamGroups);

    // asserting that:
    // (1) the assignment tokens path was created for stopping stream
    // (2) a token has ben issued for instance2, and only for instance2
    // (3) the token data is correct
    String assignmentTokensPath = KeyBuilder.datastreamAssignmentTokens(testCluster, datastreamGroup2.getTaskPrefix());
    Assert.assertTrue(zkClient.exists(assignmentTokensPath));
    List<String> tokenNodes = zkClient.getChildren(assignmentTokensPath);
    Assert.assertEquals(tokenNodes.size(), 1);
    String tokenData = zkClient.readData(
        KeyBuilder.datastreamAssignmentTokenForInstance(testCluster, datastreamGroup2.getTaskPrefix(), tokenNodes.get(0)));
    AssignmentToken token = AssignmentToken.fromJson(tokenData);
    Assert.assertEquals(token.getIssuedFor(), "instance2");
    String localInstance = adapter.getInstanceName();
    Assert.assertEquals(token.getIssuedBy(), localInstance);
  }

  @Test
  public void testInstanceClaimsAssignmentTokensProperly() throws Exception {
    String testCluster = "testInstanceClaimsAssignmentTokens";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter = createZkAdapter(testCluster);
    adapter.connect();

    // Creating datastream groups
    Datastream[] datastreams = DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType,
        "ketchupStream", "mayoStream");
    Datastream ketchupStream = datastreams[0];
    Datastream mayoStream = datastreams[1];
    DatastreamGroup ketchupDatastreamGroup = new DatastreamGroup(Collections.singletonList(ketchupStream));
    DatastreamGroup mayoDatastreamGroup = new DatastreamGroup(Collections.singletonList(mayoStream));

    // Simulating a stop request for ketchup stream
    zkClient.ensurePath(KeyBuilder.datastreamAssignmentTokens(testCluster, ketchupStream.getName()));
    // Creating two assignment tokens for the stream and adding it to the stopping datastream groups list
    zkClient.create(KeyBuilder.datastreamAssignmentTokenForInstance(testCluster,
        ketchupStream.getName(), adapter.getInstanceName()), "token", CreateMode.PERSISTENT);
    zkClient.create(KeyBuilder.datastreamAssignmentTokenForInstance(testCluster,
        ketchupStream.getName(), "someOtherInstance"), "token", CreateMode.PERSISTENT);

    List<DatastreamTask> tasks = new ArrayList<>();
    DatastreamTaskImpl task1 = new DatastreamTaskImpl();
    task1.setTaskPrefix(mayoDatastreamGroup.getTaskPrefix());
    task1.setConnectorType(connectorType);

    Map<String, List<DatastreamTask>> assignment = new HashMap<>();
    assignment.put(connectorType, tasks);
    List<DatastreamGroup> stoppingDatastreamGroups = Collections.singletonList(ketchupDatastreamGroup);

    adapter.claimAssignmentTokensOfInstance(assignment, stoppingDatastreamGroups, adapter.getInstanceName());

    // Asserting that ZkAdapter claimed token for the given instance, and given instance only
    List<String> nodes = zkClient.getChildren(
        KeyBuilder.datastreamAssignmentTokens(testCluster, ketchupStream.getName()));
    Assert.assertEquals(nodes.size(), 1);
    Assert.assertEquals(nodes.get(0), "someOtherInstance"); // adapter didn't touch other instance's token

    // Asserting that tokens will be left intact if the stopping stream has active tasks
    zkClient.create(KeyBuilder.datastreamAssignmentTokenForInstance(testCluster,
        ketchupStream.getName(), adapter.getInstanceName()), "token", CreateMode.PERSISTENT);
    DatastreamTaskImpl task2 = new DatastreamTaskImpl();
    task2.setTaskPrefix(ketchupDatastreamGroup.getTaskPrefix());
    task2.setConnectorType(connectorType);
    tasks.add(task2);

    adapter.claimAssignmentTokensOfInstance(assignment, stoppingDatastreamGroups, adapter.getInstanceName());
    nodes = zkClient.getChildren(
        KeyBuilder.datastreamAssignmentTokens(testCluster, ketchupStream.getName()));
    Assert.assertEquals(nodes.size(), 2);
  }

  @Test
  public void testSaveStateOnlyWhenTaskExists() {
    String testCluster = "testSaveStateOnlyWhenTaskExists";
    String connectorType = "connectorType";

    ZkClientInterceptingAdapter adapter = createInterceptingZkAdapter(testCluster);
    adapter.connect();

    List<DatastreamTask> tasks1 = new ArrayList<>();
    DatastreamTaskImpl dsTask = new DatastreamTaskImpl();
    dsTask.setId("task1");
    String taskPrefix = "taskPrefix1";
    dsTask.setTaskPrefix(taskPrefix);
    dsTask.setConnectorType(connectorType);
    dsTask.setZkAdapter(adapter);
    tasks1.add(dsTask);
    Map<String, List<DatastreamTask>> oldAssignments = new HashMap<>();
    oldAssignments.put(adapter.getInstanceName(), tasks1);
    adapter.updateAllAssignments(oldAssignments);

    Map<String, Set<DatastreamTask>> currentAssignments = adapter.getAllAssignedDatastreamTasks();
    Assert.assertEquals(oldAssignments.keySet(), currentAssignments.keySet());
    currentAssignments.forEach((k, v) -> Assert.assertEquals(v, new HashSet<>(oldAssignments.get(k))));

    dsTask.setStatus(DatastreamTaskStatus.ok());
    Assert.assertTrue(adapter.getZkClient().exists(KeyBuilder.datastreamTaskState(testCluster, connectorType, dsTask.getDatastreamTaskName())));

    adapter.deleteTasksWithPrefix(connectorType, taskPrefix);
    Assert.assertFalse(adapter.getZkClient().exists(KeyBuilder.datastreamTaskState(testCluster, connectorType, dsTask.getDatastreamTaskName())));
    dsTask.setStatus(DatastreamTaskStatus.ok());
    Assert.assertFalse(adapter.getZkClient().exists(KeyBuilder.datastreamTaskState(testCluster, connectorType, dsTask.getDatastreamTaskName())));

    adapter.disconnect();
  }

  @Test
  public void testTaskReassignments() {
    String testCluster = "testTaskReassignments";
    String connectorType = "connectorType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkAdapter adapter1 = createZkAdapter(testCluster);
    ZkAdapter adapter2 = createZkAdapter(testCluster);
    adapter1.connect();
    adapter2.connect();

    DatastreamTaskImpl task1 = new DatastreamTaskImpl();
    task1.setTaskPrefix("task1");
    task1.setConnectorType(connectorType);

    DatastreamTaskImpl task2 = new DatastreamTaskImpl();
    task2.setTaskPrefix("task2");
    task2.setConnectorType(connectorType);

    DatastreamTaskImpl task3 = new DatastreamTaskImpl();
    task3.setTaskPrefix("task3");
    task3.setConnectorType(connectorType);

    DatastreamTaskImpl task4 = new DatastreamTaskImpl();
    task4.setTaskPrefix("task4");
    task4.setConnectorType(connectorType);

    //
    // simulate assigning:
    //   to instance1: [task1]
    //   to instance2: [task2, task3]
    //
    Map<String, List<DatastreamTask>> assignmentsByInstance = new HashMap<>();
    assignmentsByInstance.put(adapter1.getInstanceName(), Collections.singletonList(task1));
    assignmentsByInstance.put(adapter2.getInstanceName(), Arrays.asList(task2, task3));

    adapter1.updateAllAssignments(assignmentsByInstance);

    validateConnectorTask(testCluster, connectorType, task1.getDatastreamTaskName(), adapter1.getInstanceName(), zkClient);
    validateConnectorTask(testCluster, connectorType, task2.getDatastreamTaskName(), adapter2.getInstanceName(), zkClient);
    validateConnectorTask(testCluster, connectorType, task3.getDatastreamTaskName(), adapter2.getInstanceName(), zkClient);

    //
    // simulate reassigning:
    //   to instance1: [task3, task4]
    //   to instance2: [task1, task2]
    //
    Map<String, List<DatastreamTask>> reassignmentsByInstance = new HashMap<>();
    reassignmentsByInstance.put(adapter1.getInstanceName(), Arrays.asList(task3, task4));
    reassignmentsByInstance.put(adapter2.getInstanceName(), Arrays.asList(task1, task2));

    adapter1.updateAllAssignments(reassignmentsByInstance);

    validateConnectorTask(testCluster, connectorType, task1.getDatastreamTaskName(), adapter2.getInstanceName(), zkClient);
    validateConnectorTask(testCluster, connectorType, task2.getDatastreamTaskName(), adapter2.getInstanceName(), zkClient);
    validateConnectorTask(testCluster, connectorType, task3.getDatastreamTaskName(), adapter1.getInstanceName(), zkClient);
    validateConnectorTask(testCluster, connectorType, task4.getDatastreamTaskName(), adapter1.getInstanceName(), zkClient);
  }

  /**
   * Update all datastream task assignments of a particular Brooklin instance
   * @param adapter ZooKeeper adapter to use
   * @param instance Brooklin/Coordinator instance name
   * @param assignments new datasteam tasks to assign to the Brooklin instance
   */
  public static void updateInstanceAssignment(ZkAdapter adapter, String instance, List<DatastreamTask> assignments) {
    Map<String, List<DatastreamTask>> allAssignments = new HashMap<>();
    allAssignments.put(instance, assignments);
    adapter.updateAllAssignments(allAssignments);
  }
}
