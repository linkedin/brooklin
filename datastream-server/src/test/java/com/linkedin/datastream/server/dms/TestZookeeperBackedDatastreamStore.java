package com.linkedin.datastream.server.dms;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.Assert;

import java.io.IOException;


public class TestZookeeperBackedDatastreamStore {
  private EmbeddedZookeeper _embeddedZookeeper;
  private String _zkConnectionString;
  private ZkClient _zkClient;
  private ZookeeperBackedDatastreamStore _store;

  @BeforeMethod
  public void setup() throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
    _zkClient = new ZkClient(_zkConnectionString);
    _store = new ZookeeperBackedDatastreamStore(_zkClient, "testcluster");
  }

  @AfterMethod
  public void teardown() throws IOException {
    _embeddedZookeeper.shutdown();
  }

  private Datastream generateDatastream(int seed) {
    String name = "name_" + seed;
    String connectorType = seed % 2 == 0 ? "Oracle-Change" : "Oracle-Bootstrap";
    String source = "db_" + seed;
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    DatastreamSource datastreamSource = new DatastreamSource();
    datastreamSource.setConnectionString(source);
    Datastream ds =
        new Datastream().setName(name).setConnectorType(connectorType).setSource(datastreamSource)
            .setMetadata(metadata);
    return ds;
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
    Assert.assertTrue(ds.equals(ds2));

    // recreating the same Datastream should fail
    try {
      _store.createDatastream(ds.getName(), ds);
      Assert.fail();
    } catch (DatastreamException e) {
    }

    // deleting the Datastream
    _store.deleteDatastream(ds.getName());
    Assert.assertNull(_store.getDatastream(ds.getName()));

    Assert.assertNull(_store.getDatastream(null));
  }

  /**
   * Test invalid parameters or data on DatastreamStore
   */
  @Test(expectedExceptions = DatastreamException.class)
  public void testCreateDuplicateDatastreams() throws DatastreamException {
    try {
      // This must work
      Datastream ds = generateDatastream(0);
      _store.createDatastream(ds.getName(), ds);
    } catch (DatastreamException e) {
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
