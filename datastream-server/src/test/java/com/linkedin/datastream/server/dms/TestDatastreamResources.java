package com.linkedin.datastream.server.dms;

import java.util.HashSet;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.TestDatastreamServer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;


/**
 * Test DatastreamResources with zookeeper backed DatastreamStore
 */
@Test(singleThreaded=true)
public class TestDatastreamResources {

  public static Datastream generateDatastream(int seed) {
    return generateDatastream(seed, new HashSet<>());
  }

  public static Datastream generateDatastream(int seed, Set<String> missingFields) {
    Datastream ds = new Datastream();
    if (!missingFields.contains("name")) {
      ds.setName("name_" + seed);
    }
    if (!missingFields.contains("connectorType")) {
      ds.setConnectorType(DummyConnector.CONNECTOR_TYPE);
    }
    if (!missingFields.contains("source")) {
      ds.setSource(new DatastreamSource());
      ds.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    }
    if (!missingFields.contains("target")) {
      String metadataBrokers = "kafkaBrokers_" + seed;
      String targetTopic = "kafkaTopic_" + seed;
    }
    if (!missingFields.contains("metadata")) {
      StringMap metadata = new StringMap();
      metadata.put("owner", "person_" + seed);
      ds.setMetadata(metadata);
    }
    return ds;
  }

  @BeforeTest
  public void setUp() throws Exception {
    TestDatastreamServer.initializeTestDatastreamServer(null);
  }

  @Test
  public void testReadDatastream() {
    DatastreamResources resource1 = new DatastreamResources();
    DatastreamResources resource2 = new DatastreamResources();

    // read before creating
    Datastream ds = resource1.get("name_0");
    Assert.assertNull(ds);

    CreateResponse response = resource1.create(generateDatastream(0));
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    ds = resource2.get("name_0");
    Assert.assertNotNull(ds);
    Assert.assertTrue(ds.equals(generateDatastream(0)));
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
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    missingFields.clear();
    missingFields.add("connectorType");
    Datastream noConnectorType = generateDatastream(4, missingFields);
    response = resource.create(noConnectorType);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    missingFields.clear();
    missingFields.add("source");
    Datastream noSource = generateDatastream(5, missingFields);
    response = resource.create(noSource);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    // creating existing Datastream
    response = resource.create(allRequiredFields);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_409_CONFLICT);
  }

  @Test
  public void testCreateInvalidDatastream() {
    DatastreamResources resource = new DatastreamResources();
    Datastream datastream1 = generateDatastream(6);
    datastream1.setConnectorType("InvalidConnectorName");
    CreateResponse response = resource.create(datastream1);
    Assert.assertNotNull(response.getError());
    Assert.assertEquals(response.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);

    Datastream datastream2 = generateDatastream(7);
    datastream2.setSource(new DatastreamSource());
    datastream2.getSource().setConnectionString("InvalidSource");
    CreateResponse response2 = resource.create(datastream1);
    Assert.assertNotNull(response2.getError());
    Assert.assertEquals(response2.getError().getStatus(), HttpStatus.S_400_BAD_REQUEST);
  }
}
