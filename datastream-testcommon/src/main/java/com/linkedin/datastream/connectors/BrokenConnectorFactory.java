package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;

public class BrokenConnectorFactory implements ConnectorFactory<BrokenConnector> {
  @Override
  public BrokenConnector createConnector(String connectorName, Properties config, String clusterName) {
    try {
      return new BrokenConnector(config);
    } catch (Exception e) {
      throw new RuntimeException("Instantiating BrokenConnector threw exception", e);
    }
  }
}
