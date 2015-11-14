package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.Connector;
import com.linkedin.datastream.server.ConnectorFactory;


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
