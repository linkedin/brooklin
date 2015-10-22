package com.linkedin.datastream;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.DummyDatastreamEventCollector;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.r2.RemoteInvocationException;

import junit.framework.Assert;


@Test(singleThreaded=true)
public class TestDsm {
  Logger LOG = LoggerFactory.getLogger(TestDsm.class);
  // "com.linkedin.datastream.server.DummyDatastreamEventCollector"
  private static final String COLLECTOR_CLASS = DummyDatastreamEventCollector.class.getTypeName();
  // "com.linkedin.datastream.server.connectors.DummyConnector"
  private static final String DUMMY_CONNECTOR = DummyConnector.class.getTypeName();
  // "com.linkedin.datastream.server.assignment.BroadcastStrategy"
  private static final String BROADCAST_STRATEGY =  BroadcastStrategy.class.getTypeName();

  @BeforeTest
  public void setUp() throws Exception {
    DatastreamServer.INSTANCE.shutDown();
  }

  @AfterTest
  public void tearDown() throws Exception {
    DatastreamServer.INSTANCE.shutDown();
  }

  public static Datastream generateDatastream(int seed) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorType("com.linkedin.datastream.server.connectors.DummyConnector");
    ds.setSource("db_" + seed);
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  private void setupServer()
      throws DatastreamException, IOException {
    EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper();
    String zkConnectionString = embeddedZookeeper.getConnection();
    embeddedZookeeper.startup();

    Properties properties = new Properties();
    properties.put(DatastreamServer.CONFIG_CLUSTER_NAME, "testCluster");
    properties.put(DatastreamServer.CONFIG_ZK_ADDRESS, zkConnectionString);
    properties.put(DatastreamServer.CONFIG_EVENT_COLLECTOR_CLASS_NAME, COLLECTOR_CLASS);
    properties.put(DatastreamServer.CONFIG_HTTP_PORT, "8080");
    properties.put(DatastreamServer.CONFIG_CONNECTOR_CLASS_NAMES, DUMMY_CONNECTOR);
    properties.put(DUMMY_CONNECTOR + ".assignmentStrategy", BROADCAST_STRATEGY);
    properties.put(DUMMY_CONNECTOR + ".dummyProperty", "dummyValue"); // DummyConnector will verify this value being correctly set

    DatastreamServer server = DatastreamServer.INSTANCE;
    server.init(properties);
  }

  @Test
  public void testCreateDatastream()
      throws DatastreamException, IOException, RemoteInvocationException {
    setupServer();
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    Datastream datastream = generateDatastream(1);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.createDatastream(datastream);
    Datastream createdDatastream = restClient.getDatastream(datastream.getName());
    LOG.info("Created Datastream : " + createdDatastream);
    Assert.assertEquals(createdDatastream, datastream);
  }

}
