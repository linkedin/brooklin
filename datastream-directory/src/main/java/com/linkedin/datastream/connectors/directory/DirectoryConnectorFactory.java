/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.directory;

import java.util.Properties;

import org.apache.commons.lang3.Validate;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


/**
 * Factory for creating {@link DirectoryConnector} instances.
 */
public class DirectoryConnectorFactory implements ConnectorFactory<DirectoryConnector> {
  private static final String CFG_DEFAULT_MAX_POOL_SIZE = "5";
  private static final String CFG_THREAD_POOL_SIZE = "maxExecProcessors";

  @Override
  public DirectoryConnector createConnector(String connectorName, Properties config, String clusterName) {
    Validate.notEmpty(connectorName);
    Validate.notNull(config);
    Validate.notEmpty(clusterName);

    return new DirectoryConnector(Integer.parseInt(config.getProperty(CFG_THREAD_POOL_SIZE,
        CFG_DEFAULT_MAX_POOL_SIZE)));
  }
}
