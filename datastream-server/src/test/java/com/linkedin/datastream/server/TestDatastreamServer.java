package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.connectors.DummyBootstrapConnectorFactory;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Properties;

@Test(singleThreaded=true)
public class TestDatastreamServer {
  // "com.linkedin.datastream.server.DummyDatastreamEventCollector"
  private static final String COLLECTOR_CLASS = DummyDatastreamEventCollector.class.getTypeName();
  // "com.linkedin.datastream.server.assignment.BroadcastStrategy"
  private static final String BROADCAST_STRATEGY =  BroadcastStrategy.class.getTypeName();
  private static final String DUMMY_CONNECTOR = DummyConnector.CONNECTOR_TYPE;
  private static final String DUMMY_BOOTSTRAP_CONNECTOR = DummyBootstrapConnector.CONNECTOR_TYPE;
  private static final String DUMMY_TRANSPORT_FACTORY = DummyTransportProviderFactory.class.getTypeName();

  public static Properties initializeTestDatastreamServerWithBootstrap() throws Exception {
    Properties override = new Properties();
    override.put(DatastreamServer.CONFIG_CONNECTOR_TYPES, DUMMY_CONNECTOR + "," + DUMMY_BOOTSTRAP_CONNECTOR);
    override.put(DUMMY_CONNECTOR + "." + DatastreamServer.CONFIG_CONNECTOR_BOOTSTRAP_TYPE,
                 DUMMY_BOOTSTRAP_CONNECTOR);
    override.put(DUMMY_BOOTSTRAP_CONNECTOR + "." + DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME,
                 DummyBootstrapConnectorFactory.class.getTypeName());
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
    properties.put(DatastreamServer.CONFIG_CONNECTOR_TYPES, DUMMY_CONNECTOR);
    properties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, DUMMY_TRANSPORT_FACTORY);
    properties.put(DUMMY_CONNECTOR + "." + DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY,
                   BROADCAST_STRATEGY);
    properties.put(DUMMY_CONNECTOR + "." + DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME,
                   DummyConnectorFactory.class.getTypeName());
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

  @Test
  public void testDatastreamServerMisConfig() throws Exception {
    // Wrong class name was assigned to DummyConnector; DatastreamServer should detect it and throw
    Properties override = new Properties();
    override.put(DUMMY_CONNECTOR + "." + DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME,
        DummyBootstrapConnector.class.getTypeName());
    boolean caughtException = false;
    try {
      initializeTestDatastreamServer(override);
    } catch (DatastreamException ds) {
      caughtException = true;
    }

    Assert.assertTrue(caughtException);
  }

  @Test
  public void testEndToEndHappyBasic() throws Exception {
    initializeTestDatastreamServer(null);

  }

}
