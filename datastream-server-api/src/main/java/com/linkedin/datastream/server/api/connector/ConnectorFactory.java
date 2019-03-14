/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.connector;

import java.util.Properties;


/**
 * Connector factory interface. Each connector should implement this which creates the connector instance.
 */
public interface ConnectorFactory<T extends Connector> {

  /**
   * create connector instance. Each connector should implement this method to create the corresponding connector
   * instance based on the configuration.
   * @param connectorName the connector name
   * @param config    Connector configuration.
   * @param clusterName Name of the cluster where connector will be running
   * @return Instance of the connector that is created.
   */
  T createConnector(String connectorName, Properties config, String clusterName);
}
