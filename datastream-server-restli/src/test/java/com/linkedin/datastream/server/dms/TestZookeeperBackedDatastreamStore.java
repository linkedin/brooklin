/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.dms;

import java.io.IOException;
import java.util.List;

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

  @BeforeMethod
  public void setup() throws IOException {
    String clusterName = "testcluster";
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
    _zkClient = new ZkClient(_zkConnectionString);
    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(_zkClient, clusterName);
    _store = new ZookeeperBackedDatastreamStore(datastreamCache, _zkClient, clusterName);
  }

  @AfterMethod
  public void teardown() throws IOException {
    _embeddedZookeeper.shutdown();
  }

  private Datastream generateDatastream(int seed) {
    String name = "name_" + seed;
    String connectorType = seed % 2 == 0 ? "Oracle-Change" : "Oracle-Bootstrap";
    String source = "db_" + seed;
    String dest = "topic_" + seed;
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
        .setMetadata(metadata);
  }

  /**
   * Test Datastream store with single Datastream for creating, reading, deleting
   */
  @Test
  public void testSingleDatastreamBasics() throws Exception {
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
    } catch (DatastreamAlreadyExistsException e) {
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
    long touchedTime = Long.valueOf(touchedTimestamp);
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
  public void testCreateWithNullName() throws Exception {
    _store.createDatastream(null, generateDatastream(0));
  }

  /**
   * Test invalid parameters or data on DatastreamStore
   */
  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateWithNullDatastream() throws Exception {
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
}
