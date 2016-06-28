package com.linkedin.datastream.connectors.mysql;

import java.util.Properties;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class MysqlConnectorFactory implements ConnectorFactory {
  @Override
  public Connector createConnector(Properties config) {
    try {
      return new MysqlConnector(config);
    } catch (DatastreamException e) {
      throw new DatastreamRuntimeException("Instantiating Mysql connector failed with exception", e);
    }
  }
}
