/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.util.Collections;
import java.util.Map;

import com.linkedin.datastream.server.DatastreamTask;


public class NoOpCheckpointProvider implements CheckpointProvider {
  @Override
  public void unassignDatastreamTask(DatastreamTask task) {

  }

  @Override
  public void updateCheckpoint(DatastreamTask task, int partition, String checkpoint) {
    return;
  }

  @Override
  public void flush() {
  }

  @Override
  public Map<Integer, String> getSafeCheckpoints(DatastreamTask task) {
    return Collections.emptyMap();
  }

  @Override
  public Map<Integer, String> getCommitted(DatastreamTask datastreamTask) {
    return Collections.emptyMap();
  }
}
