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
