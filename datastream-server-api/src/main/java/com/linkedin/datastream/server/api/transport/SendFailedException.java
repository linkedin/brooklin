package com.linkedin.datastream.server.api.transport;

import java.util.Map;

import com.linkedin.datastream.server.DatastreamTask;


public class SendFailedException extends Exception {
  private static final long serialVersionUID = 1;

  private Map<DatastreamTask, Map<Integer, String>> _checkpoints;

  public SendFailedException(Map<DatastreamTask, Map<Integer, String>> checkpoints) {
    _checkpoints = checkpoints;
  }

  public Map<DatastreamTask, Map<Integer, String>> getCheckpoints() {
    return _checkpoints;
  }
}
