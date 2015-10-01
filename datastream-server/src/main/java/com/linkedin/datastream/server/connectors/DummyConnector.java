package com.linkedin.datastream.server.connectors;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.Connector;
import com.linkedin.datastream.server.DatastreamContext;
import com.linkedin.datastream.server.DatastreamEventCollectorFactory;
import com.linkedin.datastream.server.DatastreamTarget;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamValidationResult;

import java.util.List;

/**
 * A trivial implementation of connector interface
 */
public class DummyConnector implements Connector {
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
    return null;
  }
}
