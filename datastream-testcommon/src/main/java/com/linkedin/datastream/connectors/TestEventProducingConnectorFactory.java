package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class TestEventProducingConnectorFactory implements ConnectorFactory<TestEventProducingConnector> {
  @Override
  public TestEventProducingConnector createConnector(String connectorName, Properties config) {
    return new TestEventProducingConnector(config);
  }
}
