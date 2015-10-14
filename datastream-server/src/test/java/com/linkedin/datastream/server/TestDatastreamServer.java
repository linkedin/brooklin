package com.linkedin.datastream.server;

import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.server.connectors.DummyConnector;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.Properties;

@Test(singleThreaded=true)
public class TestDatastreamServer {
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

  @Test
  public void testDatastreamServerBasics() throws Exception {
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

}
