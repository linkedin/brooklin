package com.linkedin.datastream.server.providers;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.zk.ZkAdapter;



public class ZookeeperCheckpointProvider implements CheckpointProvider {
  private final ZkAdapter _zkAdapter;

  private static String CHECKPOINT_KEY_NAME = "sourceCheckpoint";

  public ZookeeperCheckpointProvider(ZkAdapter zkAdapter) {
    _zkAdapter = zkAdapter;
  }

  @Override
  public void commit(Map<DatastreamTask, String> checkpoints) {
    for(DatastreamTask datastreamTask : checkpoints.keySet()) {
      _zkAdapter.setDatastreamTaskStateForKey(datastreamTask, CHECKPOINT_KEY_NAME, checkpoints.get(datastreamTask));
    }
  }

  @Override
  public Map<DatastreamTask, String> committed(List<DatastreamTask> datastreamTasks) {
    return datastreamTasks.stream().collect(
        Collectors.toMap(Function.identity(), dt -> _zkAdapter.getDatastreamTaskStateForKey(dt, CHECKPOINT_KEY_NAME)));
  }
}
