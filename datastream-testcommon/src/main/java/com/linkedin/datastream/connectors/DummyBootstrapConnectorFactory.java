package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class DummyBootstrapConnectorFactory implements ConnectorFactory {
  @Override
  public Connector createConnector(Properties config) {
    try {
      return new DummyBootstrapConnector(config);
    } catch (Exception e) {
      throw new RuntimeException("Instantiating DummyBootstrapConnector failed", e);
    }
  }
}
