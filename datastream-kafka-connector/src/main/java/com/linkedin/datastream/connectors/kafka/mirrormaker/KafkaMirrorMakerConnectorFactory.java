package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


/**
 * Factory for creating KafkaMirrorMakerConnector instances.
 */
public class KafkaMirrorMakerConnectorFactory implements ConnectorFactory<KafkaMirrorMakerConnector> {

  @Override
  public KafkaMirrorMakerConnector createConnector(String connectorName, Properties config, String clusterName) {
    return new KafkaMirrorMakerConnector(connectorName, config, clusterName);
  }
}
