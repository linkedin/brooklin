package com.linkedin.datastream.server.dms;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.KafkaConnection;
import com.linkedin.datastream.server.zk.ZkClient;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;


public class TestZookeeperBackedDatastreamStore {
  private static final Logger LOG = LoggerFactory.getLogger(TestZookeeperBackedDatastreamStore.class);

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
    String metadataBrokers = "kafkaBrokers_" + seed;
    String targetTopic = "kafkaTopic_" + seed;
    Datastream.Target target = new Datastream.Target();
    target.setKafkaConnection(new KafkaConnection().setMetadataBrokers(metadataBrokers).setTopicName(targetTopic));
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    Datastream ds =
        new Datastream().setName(name).setConnectorType(connectorType).setSource(source).setTarget(target)
            .setMetadata(metadata);
    return ds;
  }

  /**
   * Test Datastream store with single Datastream for creating, reading, deleting
   */
  @Test
  public void testSingleDatastreamBasics() {
    Datastream ds = generateDatastream(0);

    Assert.assertNull(_store.getDatastream(ds.getName()));

    // creating a Datastream
    Assert.assertTrue(_store.createDatastream(ds.getName(), ds));

    // get the same Datastream back
    Datastream ds2 = _store.getDatastream(ds.getName());
    Assert.assertNotNull(ds2);
    Assert.assertTrue(ds.equals(ds2));

    // recreating the same Datastream should fail
    Assert.assertFalse(_store.createDatastream(ds.getName(), ds));

    // deleting the Datastream
    Assert.assertTrue(_store.deleteDatastream(ds.getName()));
    Assert.assertNull(_store.getDatastream(ds.getName()));
  }

  /**
   * Test invalid parameters or data on DatastreamStore
   */
  @Test
  public void testInvalidOperations() {
    Assert.assertFalse(_store.createDatastream(null, generateDatastream(0)));
    Assert.assertFalse(_store.createDatastream("name_0", null));
    Assert.assertFalse(_store.deleteDatastream(null));
    Assert.assertNull(_store.getDatastream(null));

    _zkClient.ensurePath("/testcluster/dms/datastream1");
    String data = "Corrupted data";
    _zkClient.writeData("/testcluster/dms/datastream1", data);

    Assert.assertNull(_store.getDatastream("datastream1"));
  }
}
