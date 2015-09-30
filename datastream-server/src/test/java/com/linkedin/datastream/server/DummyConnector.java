package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;

import java.util.List;


public class DummyConnector implements Connector {
  @Override
  public void start(DatastreamEventCollector collector) {
  }

  @Override
  public void stop() {
  }

  @Override
  public String getConnectorType() {
    return "com.linkedin.datastream.server.DummyConnector";
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
