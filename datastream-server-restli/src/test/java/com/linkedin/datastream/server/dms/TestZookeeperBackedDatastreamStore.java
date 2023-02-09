/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.dms;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.CachedDatastreamReader;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.HostTargetAssignment;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;


/**
 * Tests for {@link ZookeeperBackedDatastreamStore}
 */
public class TestZookeeperBackedDatastreamStore {
  private EmbeddedZookeeper _embeddedZookeeper;
  private String _zkConnectionString;
  private ZkClient _zkClient;
  private ZookeeperBackedDatastreamStore _store;
  private String _clusterName = "testcluster";

  @BeforeMethod
  public void setup() throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
    _zkClient = new ZkClient(_zkConnectionString);
    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(_zkClient, _clusterName);
    _store = new ZookeeperBackedDatastreamStore(datastreamCache, _zkClient, _clusterName);
  }

  @AfterMethod
  public void teardown() {
    _embeddedZookeeper.shutdown();
  }

  private Datastream generateDatastream(int seed) {
    String name = "name_" + seed;
    String connectorType = seed % 2 == 0 ? "Oracle-Change" : "Oracle-Bootstrap";
    String source = "db_" + seed;
    String dest = "topic_" + seed;
    String transportProvider = "dummyTransportProvider";
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    DatastreamSource datastreamSource = new DatastreamSource();
    datastreamSource.setConnectionString(source);
    DatastreamDestination datastreamDestination = new DatastreamDestination();
    datastreamDestination.setConnectionString(dest);
    return new Datastream().setName(name)
        .setConnectorName(connectorType)
        .setSource(datastreamSource)
        .setDestination(datastreamDestination)
        .setMetadata(metadata)
        .setTransportProviderName(transportProvider);
  }

  // adds task node in zk
  private void addTaskNode(String instance, DatastreamTaskImpl task) {
    String taskPath = KeyBuilder.connectorTask(_clusterName, task.getConnectorType(), task.getDatastreamTaskName());
    _zkClient.ensurePath(taskPath);
    _zkClient.writeData(taskPath, instance);
  }

  /**
   * Test Datastream store with single Datastream for creating, reading, deleting
   */
  @Test
  public void testSingleDatastreamBasics() {
    Datastream ds = generateDatastream(0);

    Assert.assertNull(_store.getDatastream(ds.getName()));

    // creating a Datastream
    _store.createDatastream(ds.getName(), ds);

    // get the same Datastream back
    Datastream ds2 = _store.getDatastream(ds.getName());
    Assert.assertNotNull(ds2);
    Assert.assertEquals(ds2, ds);

    // recreating the same Datastream should fail
    try {
      _store.createDatastream(ds.getName(), ds);
      Assert.fail();
    } catch (DatastreamAlreadyExistsException ignored) {
    }

    // deleting the Datastream
    _store.deleteDatastream(ds.getName());
    Assert.assertEquals(_store.getDatastream(ds.getName()).getStatus(), DatastreamStatus.DELETING);

    Assert.assertNull(_store.getDatastream(null));
  }

  @Test
  public void testUpdateDatastream() throws Exception {
    Datastream ds = generateDatastream(0);

    Assert.assertNull(_store.getDatastream(ds.getName()));

    // creating a Datastream
    _store.createDatastream(ds.getName(), ds);

    // get the same Datastream back
    Datastream ds2 = _store.getDatastream(ds.getName());
    Assert.assertNotNull(ds2);
    Assert.assertEquals(ds2, ds);

    // update the datastream
    ds.getMetadata().put("key", "value");
    ds.getSource().setConnectionString("updated");
    _store.updateDatastream(ds.getName(), ds, false);

    ds2 = _store.getDatastream(ds.getName());
    Assert.assertEquals(ds, ds2);
  }

  @Test
  public void testUpdatePartitionAssignments() throws Exception {
    String datastreamGroupName = "dg1";
    Datastream ds = generateDatastream(0);
    ds.setMetadata(new StringMap());
    ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, datastreamGroupName);
    String clusterName = "testcluster";

    HostTargetAssignment targetAssignment = new HostTargetAssignment(ImmutableList.of("p-0", "p-1"), "instance1");
    _zkClient.ensurePath(KeyBuilder.instance(clusterName, "instance1-0000"));
    long startTime = System.currentTimeMillis();
    _store.updatePartitionAssignments(ds.getName(), ds, targetAssignment, true);
    long endTime = System.currentTimeMillis();

    String nodePath = KeyBuilder.getTargetAssignmentPath(clusterName, ds.getConnectorName(), datastreamGroupName);
    List<String> nodes = _zkClient.getChildren(nodePath);
    Assert.assertTrue(nodes.size() > 0);

    String touchedTimestamp = _zkClient.readData(KeyBuilder.getTargetAssignmentBase(clusterName, ds.getConnectorName()));
    long touchedTime = Long.parseLong(touchedTimestamp);
    Assert.assertTrue(endTime >= touchedTime && touchedTime >= startTime);
  }

  @Test(expectedExceptions = DatastreamException.class)
  public void testUpdatePartitionAssignmentsWithoutValidHost() throws Exception {
    String datastreamGroupName = "dg1";
    Datastream ds = generateDatastream(0);
    ds.setMetadata(new StringMap());
    ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, datastreamGroupName);
    String clusterName = "testcluster";

    HostTargetAssignment targetAssignment = new HostTargetAssignment(ImmutableList.of("p-0", "p-1"), "instance2");
    _zkClient.ensurePath(KeyBuilder.instance(clusterName, "instance1-0000"));
    _store.updatePartitionAssignments(ds.getName(), ds, targetAssignment, true);
  }

  @Test
  public void testUpdatePartitionAssignmentsWithPausedInstances() throws Exception {
    String datastreamGroupName = "dg1";
    Datastream ds = generateDatastream(0);
    ds.setMetadata(new StringMap());
    ds.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, datastreamGroupName);
    String clusterName = "testcluster";

    HostTargetAssignment targetAssignment = new HostTargetAssignment(ImmutableList.of("p-0", "p-1"), "instance1");
    _zkClient.ensurePath(KeyBuilder.instance(clusterName, "instance1-0000"));
    _zkClient.ensurePath(KeyBuilder.instance(clusterName, "PAUSED_INSTANCE"));
    _store.updatePartitionAssignments(ds.getName(), ds, targetAssignment, true);
  }

  /**
   * Test invalid parameters or data on DatastreamStore
   */
  @Test(expectedExceptions = DatastreamAlreadyExistsException.class)
  public void testCreateDuplicateDatastreams() {
    try {
      // This must work
      Datastream ds = generateDatastream(0);
      _store.createDatastream(ds.getName(), ds);
    } catch (DatastreamAlreadyExistsException e) {
      Assert.fail();
    }

    // This should throw
    Datastream ds = generateDatastream(0);
    _store.createDatastream(ds.getName(), ds);
  }

  /**
   * Test invalid parameters or data on DatastreamStore
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateWithNullName() {
    _store.createDatastream(null, generateDatastream(0));
  }

  /**
   * Test invalid parameters or data on DatastreamStore
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateWithNullDatastream() {
    _store.createDatastream("name_0", null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDeleteWithNullDatastream() {
    _store.deleteDatastream(null);
  }

  @Test
  public void testGetCorruptedDatastream() {
    _zkClient.ensurePath("/testcluster/dms/datastream1");
    String data = "Corrupted data";
    _zkClient.writeData("/testcluster/dms/datastream1", data);
    Assert.assertNull(_store.getDatastream("datastream1"));
  }

  /**
   * Test Datastream store with single Datastream for creating, reading, deleting
   */
  @Test
  public void testSingleDatastreamBasicsWithNumTasks() throws DatastreamException {
    Datastream ds = generateDatastream(0);

    Assert.assertNull(_store.getDatastream(ds.getName()));

    // creating a Datastream
    _store.createDatastream(ds.getName(), ds);

    _zkClient.create(KeyBuilder.datastreamNumTasks(_clusterName, ds.getName()), "10", CreateMode.PERSISTENT);

    // get the same Datastream back
    Datastream ds2 = _store.getDatastream(ds.getName());
    Assert.assertNotNull(ds2);
    Assert.assertNotEquals(ds2, ds);
    Objects.requireNonNull(ds.getMetadata()).put("numTasks", "10");
    Assert.assertEquals(ds2, ds);

    // try modifying the numTasks. It should not work.
    Objects.requireNonNull(ds.getMetadata()).put("numTasks", "20");
    _store.updateDatastream(ds.getName(), ds, false);
    // get the same Datastream back
    Datastream ds3 = _store.getDatastream(ds.getName());
    Assert.assertNotNull(ds3);
    Assert.assertNotEquals(ds3, ds);
    Assert.assertEquals(ds3.getMetadata().get("numTasks"), "10");

    // deleting the Datastream
    _store.deleteDatastream(ds.getName());
    Assert.assertEquals(_store.getDatastream(ds.getName()).getStatus(), DatastreamStatus.DELETING);

    Assert.assertNull(_store.getDatastream(null));
  }

  @Test
  public void testGetAssigneeNodeHappyPath() {
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    _store.createDatastream(datastreamName, datastreamToCreate);

    String dummyInstance = "DUMMY_INSTANCE";

    DatastreamTaskImpl dummyTask = new DatastreamTaskImpl(new ArrayList<>(Collections.singletonList(datastreamToCreate)));
    addTaskNode(dummyInstance, dummyTask);

    Assert.assertEquals(dummyInstance, _store.getAssignedTaskInstance(datastreamName, dummyTask.getDatastreamTaskName()));
  }

  @Test
  public void testGetAssigneeNodeMissingDatastream() {
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();

    String dummyInstance = "DUMMY_INSTANCE";

    DatastreamTaskImpl dummyTask = new DatastreamTaskImpl(new ArrayList<>(Collections.singletonList(datastreamToCreate)));
    addTaskNode(dummyInstance, dummyTask);

    Assert.assertNull(_store.getAssignedTaskInstance(datastreamName, dummyTask.getDatastreamTaskName()));
  }

  @Test
  public void testGetAssigneeNodeMissingTask() {
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();

    String dummyInstance = "DUMMY_INSTANCE";

    DatastreamTaskImpl dummyTask = new DatastreamTaskImpl(new ArrayList<>(Collections.singletonList(datastreamToCreate)));
    addTaskNode(dummyInstance, dummyTask);

    Assert.assertNull(_store.getAssignedTaskInstance(datastreamName, null));
  }

  @Test
  public void testDeleteAssignmentTokens() {
    Datastream ds = generateDatastream(0);
    _store.createDatastream(ds.getName(), ds);

    // Create assignment tokens under datastream
    String tokensRootPath = KeyBuilder.datastreamAssignmentTokens(_clusterName, ds.getName());
    String token1Path = KeyBuilder.datastreamAssignmentTokenForInstance(_clusterName, ds.getName(), "instance1");
    String token2Path = KeyBuilder.datastreamAssignmentTokenForInstance(_clusterName, ds.getName(), "instance2");
    _zkClient.create(tokensRootPath, null, CreateMode.PERSISTENT);
    _zkClient.create(token1Path, null, CreateMode.PERSISTENT);
    _zkClient.create(token2Path, null, CreateMode.PERSISTENT);

    // Make sure nodes for tokens were created
    Assert.assertTrue(_zkClient.exists(tokensRootPath));
    Assert.assertTrue(_zkClient.exists(token1Path));
    Assert.assertTrue(_zkClient.exists(token2Path));

    _store.forceCleanupDatastream(ds.getName());

    // Make sure nodes for tokens were deleted
    Assert.assertFalse(_zkClient.exists(tokensRootPath));
    Assert.assertFalse(_zkClient.exists(token1Path));
    Assert.assertFalse(_zkClient.exists(token2Path));
  }
}
