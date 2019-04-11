/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;

/**
 * Factory class for creating instances of {@link DummyBootstrapConnector}
 */
public class DummyBootstrapConnectorFactory implements ConnectorFactory<DummyBootstrapConnector> {
  @Override
  public DummyBootstrapConnector createConnector(String connectorName, Properties config, String clusterName) {
    try {
      return new DummyBootstrapConnector(config);
    } catch (Exception e) {
      throw new RuntimeException("Instantiating DummyBootstrapConnector failed", e);
    }
  }
}
