package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class DummyBootstrapConnectorFactory implements ConnectorFactory<DummyBootstrapConnector> {
  @Override
  public DummyBootstrapConnector createConnector(String connectorName, Properties config) {
    try {
      return new DummyBootstrapConnector(config);
    } catch (Exception e) {
      throw new RuntimeException("Instantiating DummyBootstrapConnector failed", e);
    }
  }
}
