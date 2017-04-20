package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.util.Properties;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class OracleConnectorFactory implements ConnectorFactory<OracleConnector> {
  public OracleConnector createConnector(String connectorName, Properties config) {
    try {
      return new OracleConnector(config);
    } catch (DatastreamException e) {
      throw new RuntimeException("Oracle TriggerBased Connector failed to initialize", e);
    }
  }
}

