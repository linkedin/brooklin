/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;

/**
 * Factory class for creating instances of {@link TestEventProducingConnector}
 */
public class TestEventProducingConnectorFactory implements ConnectorFactory<TestEventProducingConnector> {
  @Override
  public TestEventProducingConnector createConnector(String connectorName, Properties config, String clusterName) {
    return new TestEventProducingConnector(config);
  }
}
