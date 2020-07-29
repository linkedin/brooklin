/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.jdbc;

import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;

/**
 * implementation of {@link ConnectorFactory} for {@link JDBCConnector}
 */
public class JDBCConnectorFactory implements ConnectorFactory<JDBCConnector>  {
    @Override
    public JDBCConnector createConnector(String connectorName, Properties config, String clusterName) {
        return new JDBCConnector(new VerifiableProperties(config));
    }
}
