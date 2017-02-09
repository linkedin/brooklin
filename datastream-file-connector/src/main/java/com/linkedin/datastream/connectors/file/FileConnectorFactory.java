package com.linkedin.datastream.connectors.file;

import java.util.Properties;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;


public class FileConnectorFactory implements ConnectorFactory<FileConnector> {
  @Override
  public FileConnector createConnector(String connectorName, Properties config) {
    try {
      return new FileConnector(config);
    } catch (DatastreamException e) {
      throw new RuntimeException("File connector instantiation failed with error", e);
    }
  }
}
