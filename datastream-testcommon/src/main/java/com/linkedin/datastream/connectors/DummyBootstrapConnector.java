package com.linkedin.datastream.connectors;

import java.util.List;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * A trivial implementation of connector interface
 */
public class DummyBootstrapConnector implements Connector {

  private Properties _properties;

  public static final String CONNECTOR_TYPE = "DummyConnector-Bootstrap";

  public DummyBootstrapConnector(Properties properties) throws Exception {
    _properties = properties;
  }

  @Override
  public void start() {
  }

  @Override
  public void stop() {
  }

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {

  }

  @Override
  public void initializeDatastream(Datastream stream) throws DatastreamValidationException {
    if (stream == null || stream.getSource() == null) {
      throw new DatastreamValidationException("Failed to get source from datastream.");
    }
  }
}
