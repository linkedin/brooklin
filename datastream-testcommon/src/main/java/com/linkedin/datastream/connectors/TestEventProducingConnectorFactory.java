package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class TestEventProducingConnectorFactory implements ConnectorFactory {
  @Override
  public Connector createConnector(String connectorName, Properties config) {
    return new TestEventProducingConnector(config);
  }
}
