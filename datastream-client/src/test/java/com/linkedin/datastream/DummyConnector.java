package com.linkedin.datastream;

import java.util.List;
import java.util.Properties;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.Connector;
import com.linkedin.datastream.server.DatastreamContext;
import com.linkedin.datastream.server.DatastreamEventCollectorFactory;
import com.linkedin.datastream.server.DatastreamTarget;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamValidationResult;


/**
 * A trivial implementation of connector interface
 */
public class DummyConnector implements Connector {

  private Properties _properties;

  public DummyConnector(Properties properties) throws Exception {
    _properties = properties;
    String dummyConfigValue = _properties.getProperty("dummyProperty", "");
    if (!dummyConfigValue.equals("dummyValue")) {
      throw new Exception("Invalid config value for dummyProperty. Expected: dummyValue");
    }
  }

  @Override
  public void start(DatastreamEventCollectorFactory factory) {
  }

  @Override
  public void stop() {
  }

  @Override
  public String getConnectorType() {
    return "com.linkedin.datastream.server.connectors.DummyConnector";
  }

  @Override
  public void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {

  }

  @Override
  public DatastreamTarget getDatastreamTarget(Datastream stream) {
    return null;
  }

  @Override
  public DatastreamValidationResult validateDatastream(Datastream stream) {
    return new DatastreamValidationResult();
  }
}
