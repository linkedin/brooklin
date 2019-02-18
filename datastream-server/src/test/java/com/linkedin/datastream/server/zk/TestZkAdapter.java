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
import java.util.List;

import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;


public class TestZkAdapter {
  private static final Logger LOG = LoggerFactory.getLogger(TestZkAdapter.class);
  private static final int ZK_WAIT_IN_MS = 500;

  private String defaultTransportProviderName = "test";
  private EmbeddedZookeeper _embeddedZookeeper;
  private String _zkConnectionString;

  @BeforeMethod
  public void setup() throws IOException {
    // each embedded zookeeper should be on different port
    // so the tests can run in parallel
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
  }

  @AfterMethod
  public void teardown() throws IOException {
    _embeddedZookeeper.shutdown();
  }

  @Test
  public void testInstanceName() throws Exception {
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
    return new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName,  ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, null);
  }

  @Test
  public void testSmoke() throws Exception {
    String testCluster = "test_adapter_smoke";

    ZkAdapter adapter1 = createZkAdapter(testCluster);
    adapter1.connect();

    ZkAdapter adapter2 = createZkAdapter(testCluster);
    adapter2.connect();

    // verify the zookeeper path exists for the two live instance nodes
    ZkClient client = new ZkClient(_zkConnectionString);

    List<String> instances = client.getChildren(KeyBuilder.instances(testCluster));

    Assert.assertEquals(instances.size(), 2);

    adapter1.disconnect();
    adapter2.disconnect();

    client.close();
  }

  @Test
  public void testLeaderElection() throws Exception {
    String testCluster = "test_adapter_leader";

    //
    // start two ZkAdapters, which is corresponding to two Coordinator instances
    //
    ZkAdapter adapter1 = new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName, 1000, 15000, null);
    adapter1.connect();

    ZkAdapter adapter2 = new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName, 1000, 15000, null);
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
    ZkAdapter adapter3 = new ZkAdapter(_zkConnectionString, testCluster, defaultTransportProviderName, 1000, 15000, null);
    adapter3.connect();

    //
    // verify that the adapter3 is the current leader
    //
    Assert.assertTrue(PollUtils.poll(adapter3::isLeader, 100, ZK_WAIT_IN_MS));
  }

  @Test
  public void testStressLeaderElection() throws Exception {
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

  private void validateConnectorTask(String cluster, String connectorType, String task, ZkClient zkClient) {
    List<String> connectorAssignment = zkClient.getChildren(KeyBuilder.connectorTask(cluster, connectorType, task));
    Assert.assertEquals(connectorAssignment.size(), 2); // state + config
    Assert.assertTrue(connectorAssignment.contains("state"));
    Assert.assertTrue(connectorAssignment.contains("config"));
  }

  // When Coordinator leader writes the assignment to a specific instance, the change is indeed
  // persisted in zookeeper
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
    // verify that there are 1 znode under the zookeeper /{cluster}/instances/{instance}/
    //
    List<String> assignment =
        zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter.getInstanceName()));
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 1, 100, 5000, assignment));
    Assert.assertEquals(assignment.get(0), "task1");
    validateConnectorTask(testCluster, connectorType, "task1", zkClient);

    //
    // simulate assigning two tasks [task1, task2] to the same instance
    //
    DatastreamTaskImpl task2 = new DatastreamTaskImpl();
    task2.setTaskPrefix("task2");
    task2.setConnectorType(connectorType);
    tasks.add(task2);
    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);
    //
    // verify that there are 2 znodes under the zookeeper path /{cluster}/instances/{instance}
    //
    assignment = zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter.getInstanceName()));
    Collections.sort(assignment);
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 2, 100, 5000, assignment));
    Assert.assertEquals(assignment.get(0), "task1");
    Assert.assertEquals(assignment.get(1), "task2");
    validateConnectorTask(testCluster, connectorType, "task1", zkClient);
    validateConnectorTask(testCluster, connectorType, "task2", zkClient);

    //
    // simuate removing task2 and adding task3 to the assignment, now the tasks are [task1, task3]
    //
    DatastreamTaskImpl task3 = new DatastreamTaskImpl();
    task3.setTaskPrefix("task3");
    task3.setConnectorType(connectorType);
    tasks.add(task3);
    tasks.remove(task2);
    updateInstanceAssignment(adapter, adapter.getInstanceName(), tasks);
    //
    // verify that there are still 2 znodes under zookeeper path /{cluster}/instances/{instance}
    //
    assignment = zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter.getInstanceName()));
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 2, 100, 5000, assignment));
    Collections.sort(assignment);
    Assert.assertEquals(assignment.get(0), "task1");
    Assert.assertEquals(assignment.get(1), "task3");
    validateConnectorTask(testCluster, connectorType, "task1", zkClient);
    validateConnectorTask(testCluster, connectorType, "task3", zkClient);

    //
    // cleanup
    //
    zkClient.close();
  }

  // When Coordinator leader writes the assignment to a specific instance, the change is indeed
  // persisted in zookeeper
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

    List<DatastreamTask> tasks = new ArrayList<>();

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
    assignmentsByInstance.put(adapter1.getInstanceName(), Arrays.asList(task1));
    assignmentsByInstance.put(adapter2.getInstanceName(), Arrays.asList(task2, task3));

    // Single call to update all assignments.
    adapter1.updateAllAssignments(assignmentsByInstance);

    //
    // verify that there are 1 znode under the zookeeper /{cluster}/instances/{instance1}/
    //
    List<String> assignment1 =
        zkClient.getChildren(KeyBuilder.instanceAssignments(testCluster, adapter1.getInstanceName()));
    Assert.assertTrue(PollUtils.poll((assn) -> assn.size() == 1, 100, 5000, assignment1));
    Assert.assertEquals(assignment1.get(0), "task1");
    validateConnectorTask(testCluster, connectorType, "task1", zkClient);

    //
    // verify that there are 2 znode under the zookeeper /{cluster}/instances/{instance2}/
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
  // CHECKSTYLE:OFF
  public void testInstanceAssignmentWithPartitions() throws Exception {
    String testCluster = "testUpdateInstanceAssignment";
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
    // verify there are 4 znodes under zookeeper path /{cluster}/instances/{instance}
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
    return exception ? exceptionSeen : !exceptionSeen;
  }

  @Test
  public void testTaskAquireRelease() throws Exception {
    String testCluster = "testTaskAquireRelease";
    String connectorType = "connectorType";
    Duration timeout = Duration.ofMinutes(1);

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
  public void testTaskAquireReleaseOwnerUncleanShutdown() throws Exception {
    String testCluster = "testTaskAquireReleaseOwnerUncleanShutdown";
    String connectorType = "connectorType";
    Duration timeout = Duration.ofMinutes(1);

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
  public void testTaskAquireReleaseOwnerUncleanBounce() throws Exception {
    String testCluster = "testTaskAquireReleaseOwnerUncleanBounce";
    String connectorType = "connectorType";
    Duration timeout = Duration.ofMinutes(1);

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
    PollUtils.poll(() -> adapter2.isLeader(), 50, 5000);

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


  public static void updateInstanceAssignment(ZkAdapter adapter, String instance, List<DatastreamTask> assignments) {
    Map<String, List<DatastreamTask>> allAssignments = new HashMap<>();
    allAssignments.put(instance, assignments);
    adapter.updateAllAssignments(allAssignments);
  }
}
