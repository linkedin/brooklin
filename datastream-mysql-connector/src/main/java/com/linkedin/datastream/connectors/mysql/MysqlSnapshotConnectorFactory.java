package com.linkedin.datastream.connectors.mysql;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class MysqlSnapshotConnectorFactory implements ConnectorFactory<MysqlSnapshotConnector> {

  @Override
  public MysqlSnapshotConnector createConnector(String connectorName, Properties config, String clusterName) {
    return new MysqlSnapshotConnector(config);
  }
}
