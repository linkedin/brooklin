package com.linkedin.datastream.server;

import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.server.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.server.connectors.DummyConnector;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import org.testng.annotations.Test;

import java.util.Properties;

@Test(singleThreaded=true)
public class TestDatastreamServer {
  // "com.linkedin.datastream.server.DummyDatastreamEventCollector"
  private static final String COLLECTOR_CLASS = DummyDatastreamEventCollector.class.getTypeName();
  // "com.linkedin.datastream.server.connectors.DummyConnector"
  private static final String DUMMY_CONNECTOR = DummyConnector.class.getTypeName();
  // "com.linkedin.datastream.server.connectors.DummyBootstrapConnector"
  private static final String DUMMY_BOOTSTRAP_CONNECTOR = DummyBootstrapConnector.class.getTypeName();
  // "com.linkedin.datastream.server.assignment.BroadcastStrategy"
  private static final String BROADCAST_STRATEGY =  BroadcastStrategy.class.getTypeName();

  public static Properties initializeTestDatastreamServerWithBootstrap() throws Exception {
    Properties override = new Properties();
    override.put(DatastreamServer.CONFIG_CONNECTOR_CLASS_NAMES, DUMMY_CONNECTOR + "," + DUMMY_BOOTSTRAP_CONNECTOR);
    override.put(DUMMY_CONNECTOR + ".bootstrapConnector", DUMMY_BOOTSTRAP_CONNECTOR);
    return initializeTestDatastreamServer(override);
  }

  public static Properties initializeTestDatastreamServer(Properties override) throws Exception {
    DatastreamServer.INSTANCE.shutDown();
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

    if (override != null) {
      override.entrySet().forEach(entry -> properties.put(entry.getKey(), entry.getValue()));
    }

    DatastreamServer.INSTANCE.init(properties);
    return properties;
  }

  @Test
  public void testDatastreamServerBasics() throws Exception {
    initializeTestDatastreamServer(null);
    initializeTestDatastreamServerWithBootstrap();
  }

}
