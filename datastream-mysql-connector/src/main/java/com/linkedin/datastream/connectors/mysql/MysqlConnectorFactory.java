package com.linkedin.datastream.connectors.mysql;

import java.util.Properties;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class MysqlConnectorFactory implements ConnectorFactory<MysqlConnector> {
  @Override
  public MysqlConnector createConnector(String connectorName, Properties config, String clusterName) {
    try {
      return new MysqlConnector(config);
    } catch (DatastreamException e) {
      throw new DatastreamRuntimeException("Instantiating Mysql connector failed with exception", e);
    }
  }
}
