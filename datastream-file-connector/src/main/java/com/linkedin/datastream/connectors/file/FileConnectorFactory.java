/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.file;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


/**
 * A factory for creating {@link FileConnector} instances
 */
public class FileConnectorFactory implements ConnectorFactory<FileConnector> {
  @Override
  public FileConnector createConnector(String connectorName, Properties config, String clusterName) {
    return new FileConnector(connectorName, config);
  }
}
