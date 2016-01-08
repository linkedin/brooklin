package com.linkedin.datastream.testutil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.providers.CheckpointProvider;


public class InMemoryCheckpointProvider implements CheckpointProvider {
  private Map<DatastreamTask, String> _cpMap = new HashMap<>();

  @Override
  public void commit(Map<DatastreamTask, String> checkpoints) {
    if (checkpoints.size() != 0) {
      _cpMap.putAll(checkpoints);
    }
  }

  @Override
  public Map<DatastreamTask, String> getCommitted(List<DatastreamTask> datastreamTasks) {
    Map<DatastreamTask, String> ret = new HashMap<>();
    for (DatastreamTask task : datastreamTasks) {
      if (_cpMap.containsKey(task)) {
        ret.put(task, _cpMap.get(task));
      }
    }
    return ret;
  }
}
