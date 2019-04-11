/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

import java.util.Map;

import com.linkedin.datastream.server.DatastreamTask;

/**
 * Exception used when failing to send data
 */
public class SendFailedException extends Exception {
  private static final long serialVersionUID = 1;
  private final DatastreamTask _datastreamTask;

  private Map<Integer, String> _checkpoints;

  /**
   * Construct an instance of SendFailedException
   */
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
