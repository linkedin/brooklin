/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


/**
 * Factory class for creating instances of {@link KafkaConnector}
 */
public class KafkaConnectorFactory implements ConnectorFactory<KafkaConnector> {
  @Override
  public KafkaConnector createConnector(String connectorName, Properties config, String clusterName) {
    return new KafkaConnector(connectorName, config, clusterName);
  }
}
