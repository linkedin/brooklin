package com.linkedin.datastream.connectors;

import java.util.List;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamValidationResult;


/**
 * A trivial implementation of connector interface
 */
public class DummyConnector implements Connector {

  public static final String VALID_DUMMY_SOURCE = "DummySource";
  public static final String CONNECTOR_TYPE = "DummyConnector-Online";

  private Properties _properties;

  public DummyConnector(Properties properties) throws Exception {
    _properties = properties;
    String dummyConfigValue = _properties.getProperty("dummyProperty", "");
    if (!dummyConfigValue.equals("dummyValue")) {
      throw new Exception("Invalid config value for dummyProperty. Expected: dummyValue");
    }
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
  public DatastreamValidationResult validateDatastream(Datastream stream) {
    if (stream == null || stream.getSource() == null) {
      return new DatastreamValidationResult("Failed to get source from datastream.");
    }
    if (!stream.getSource().getConnectionString().equals(VALID_DUMMY_SOURCE)) {
      return new DatastreamValidationResult("Invalid source (" + stream.getSource() + ") in datastream.");
    }
    return new DatastreamValidationResult();
  }
}
