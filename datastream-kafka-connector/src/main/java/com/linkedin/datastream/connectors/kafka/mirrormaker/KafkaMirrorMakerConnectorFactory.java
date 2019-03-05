/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
