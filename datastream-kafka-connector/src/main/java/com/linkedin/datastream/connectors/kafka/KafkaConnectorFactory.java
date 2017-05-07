package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class KafkaConnectorFactory implements ConnectorFactory<KafkaConnector> {
  @Override
  public KafkaConnector createConnector(String connectorName, Properties config) {
    return new KafkaConnector(connectorName, config);
  }
}
