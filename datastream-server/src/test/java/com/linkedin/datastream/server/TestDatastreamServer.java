package com.linkedin.datastream.server;

import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import org.testng.annotations.Test;

import java.util.Properties;


public class TestDatastreamServer {
  private static final String COLLECTOR_CLASS = "com.linkedin.datastream.server.DummyDatastreamEventCollector";

  @Test
  public void testDatastreamServerBasics() throws Exception {
    EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper();
    String zkConnectionString = embeddedZookeeper.getConnection();
    embeddedZookeeper.startup();

    Properties properties = new Properties();
    properties.put("datastream.server.coordinator.cluster", "testCluster");
    properties.put("datastream.server.coordinator.zkAddress", zkConnectionString);
    properties.put("datastream.server.eventCollectorClass", COLLECTOR_CLASS);
    properties.put("datastream.server.httpport", "8080");
    properties.put("datastream.server.connectorTypes", "com.linkedin.datastream.server.connectors.DummyConnector");
    properties.put("com.linkedin.datastream.server.DummyConnector.assignmentStrategy",
        "com.linkedin.datastream.server.assignment.BroadcastStrategy");

    DatastreamServer server = DatastreamServer.INSTANCE;
    server.init(properties);
  }
}
