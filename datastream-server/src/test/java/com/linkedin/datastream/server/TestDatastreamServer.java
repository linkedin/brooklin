package com.linkedin.datastream.server;

import org.testng.annotations.Test;

import java.util.Properties;


public class TestDatastreamServer {

  @Test
  public void testDatastreamServerBasics() throws Exception {
    Properties properties = new Properties();
    properties.put("datastream.server.coordinator.cluster", "testCluster");
    properties.put("datastream.server.coordinator.zkAddress", "localhost");
    properties.put("datastream.server.httpport", "8080");
    properties.put("datastream.server.connectorTypes", "com.linkedin.datastream.server.DummyConnector");
    properties.put("com.linkedin.datastream.server.DummyConnector.assignmentStrategy",
        "com.linkedin.datastream.server.assignment.BroadcastStrategy");

    DatastreamServer server = DatastreamServer.INSTANCE;
    server.init(properties);
  }
}
