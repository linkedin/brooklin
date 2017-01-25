package com.linkedin.datastream.server.api.transport;

import java.util.Map;

import com.linkedin.datastream.server.DatastreamTask;


public class SendFailedException extends Exception {
  private static final long serialVersionUID = 1;
  private final DatastreamTask _datastreamTask;

  private Map<Integer, String> _checkpoints;

  public SendFailedException(DatastreamTask datastreamTask, Map<Integer, String> checkpoints, Exception exception) {
    super(exception);
    _datastreamTask = datastreamTask;
    _checkpoints = checkpoints;
  }

  public Map<Integer, String> getCheckpoints() {
    return _checkpoints;
  }

  public DatastreamTask getDatastreamTask() {
    return _datastreamTask;
  }
}
