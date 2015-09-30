package com.linkedin.datastream.server.dms;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.KafkaConnection;

import com.linkedin.datastream.server.zk.ZkClient;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;
import junit.framework.Assert;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;


/**
 * Test DatastreamResources with zookeeper backed DatastreamStore
 */
public class TestDatastreamResources {

  private EmbeddedZookeeper _embeddedZookeeper;
  private String _zkConnectionString;
  private ZkClient _zkClient;
  private ZookeeperBackedDatastreamStore _store;

  private Datastream generateDatastream(int seed) {
    return generateDatastream(seed, new HashSet<>());
  }

  private Datastream generateDatastream(int seed, Set<String> missingFields) {
    Datastream ds = new Datastream();
    if (!missingFields.contains("name")) {
      ds.setName("name_" + seed);
    }
    if (!missingFields.contains("connectorType")) {
      ds.setConnectorType(seed % 2 == 0 ? "Oracle-Change" : "Oracle-Bootstrap");
    }
    if (!missingFields.contains("source")) {
      ds.setSource("db_" + seed);
    }
    if (!missingFields.contains("target")) {
      String metadataBrokers = "kafkaBrokers_" + seed;
      String targetTopic = "kafkaTopic_" + seed;
      Datastream.Target target = new Datastream.Target();
      target.setKafkaConnection(new KafkaConnection().setMetadataBrokers(metadataBrokers).setTopicName(targetTopic));
      ds.setTarget(target);
    }
    if (!missingFields.contains("metadata")) {
      StringMap metadata = new StringMap();
      metadata.put("owner", "person_" + seed);
      ds.setMetadata(metadata);
    }
    return ds;
  }

  @BeforeMethod
  public void setUp() throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
    _zkClient = new ZkClient(_zkConnectionString);
    _store = new ZookeeperBackedDatastreamStore(_zkClient, "/testcluster");
    DatastreamStoreProvider.setDatastreamStore(_store);
  }

  @Test
  public void testReadDatastream() {
    DatastreamResources resource1 = new DatastreamResources();
    DatastreamResources resource2 = new DatastreamResources();

    // read before creating
    Datastream ds = resource1.get("name_1");
    Assert.assertNull(ds);

    CreateResponse response = resource1.create(generateDatastream(1));
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    ds = resource2.get("name_1");
    Assert.assertNotNull(ds);
    Assert.assertTrue(ds.equals(generateDatastream(1)));
  }

  @Test
  public void testCreateDatastream() {
    DatastreamResources resource = new DatastreamResources();
    Set<String> missingFields = new HashSet<>();

    // happy path
    Datastream fullDatastream = generateDatastream(1);
    CreateResponse response = resource.create(fullDatastream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    missingFields.add("target");
    missingFields.add("metadata");
    Datastream allRequiredFields = generateDatastream(2, missingFields);
    response = resource.create(allRequiredFields);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // missing necessary fields
    missingFields.clear();
    missingFields.add("name");
    Datastream noName = generateDatastream(3, missingFields);
    response = resource.create(noName);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_406_NOT_ACCEPTABLE);

    missingFields.clear();
    missingFields.add("connectorType");
    Datastream noConnectorType = generateDatastream(4, missingFields);
    response = resource.create(noConnectorType);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_406_NOT_ACCEPTABLE);

    missingFields.clear();
    missingFields.add("source");
    Datastream noSource = generateDatastream(5, missingFields);
    response = resource.create(noSource);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_406_NOT_ACCEPTABLE);

    // creating existing Datastream
    response = resource.create(allRequiredFields);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_409_CONFLICT);
  }
}
