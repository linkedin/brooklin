package com.linkedin.datastream.server;

import com.linkedin.datastream.server.DatastreamTask;


public interface DatastreamContext {
  // obtain the instance name
  String getInstanceName();

  // obtain the last known persisted DatastreamState. The Connector
  // implementation can use this method to obtain the last know
  // checkpoint.
  String getState(DatastreamTask datastream, String key);

  // persiste the datastreamstate. The Connector implementation can
  // use this method to persist the last known checkpoint in zookeeper
  void saveState(DatastreamTask datastream, String key, String value);

  // set the status for datastreamtask. This is a way for the connector
  // implementation to persist the status of the datastream task
  void setStatus(DatastreamTask datastreamTask, DatastreamTaskStatus status);

  DatastreamTaskStatus getStatus(DatastreamTask datastreamTask);
}
