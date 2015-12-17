package com.linkedin.datastream.server;

import com.linkedin.datastream.connectors.DummyBootstrapConnectorFactory;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.testutil.EmbeddedDatastreamCluster;

import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Test(singleThreaded=true)
public class TestDatastreamServer {
  // "com.linkedin.datastream.server.DummyDatastreamEventCollector"
  // "com.linkedin.datastream.server.assignment.BroadcastStrategy"
  private static final String BROADCAST_STRATEGY =  BroadcastStrategy.class.getTypeName();
  private static final String DUMMY_CONNECTOR = DummyConnector.CONNECTOR_TYPE;
  private static final String DUMMY_BOOTSTRAP_CONNECTOR = DummyBootstrapConnector.CONNECTOR_TYPE;

  public static EmbeddedDatastreamCluster initializeTestDatastreamServerWithBootstrap() throws Exception {
    DatastreamServer.INSTANCE.shutdown();
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(DUMMY_CONNECTOR, getDummyConnectorProperties(true));
    connectorProperties.put(DUMMY_BOOTSTRAP_CONNECTOR, getBootstrapConnectorProperties());
    return EmbeddedDatastreamCluster.newTestDatastreamKafkaCluster(connectorProperties, new Properties(),  -1);
  }

  private static Properties getBootstrapConnectorProperties() {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, BROADCAST_STRATEGY);
    props.put(DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyBootstrapConnectorFactory.class.getTypeName());
    return props;
  }

  public static EmbeddedDatastreamCluster initializeTestDatastreamServer(Properties override) throws Exception {
    DatastreamServer.INSTANCE.shutdown();
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(DUMMY_CONNECTOR, getDummyConnectorProperties(false));
    EmbeddedDatastreamCluster
        datastreamKafkaCluster = EmbeddedDatastreamCluster.newTestDatastreamKafkaCluster(connectorProperties, override, -1);
    return datastreamKafkaCluster;
  }

  private static Properties getDummyConnectorProperties(boolean boostrap) {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, BROADCAST_STRATEGY);
    props.put(DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyConnectorFactory.class.getTypeName());
    if(boostrap) {
      props.put(DatastreamServer.CONFIG_CONNECTOR_BOOTSTRAP_TYPE, DUMMY_BOOTSTRAP_CONNECTOR);
    }
    props.put("dummyProperty", "dummyValue");
    return props;
  }

  @Test
  public void testDatastreamServerBasics() throws Exception {
    initializeTestDatastreamServer(null);
    initializeTestDatastreamServerWithBootstrap();
  }

  @Test
  public void testEndToEndHappyBasic() throws Exception {
    initializeTestDatastreamServer(null);

  }

}
