package com.linkedin.datastream.server.providers;

import java.util.List;
import java.util.Map;
import java.util.Objects;
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

  /**
   * Commit the checkpoints to the checkpoint store.
   * @param checkpoints Map of the datastreamTask to the checkpoint associated with the datastreamTask
   */
  @Override
  public void commit(Map<DatastreamTask, String> checkpoints) {
    Objects.requireNonNull(checkpoints, "Checkpoints should not be null");
    for(DatastreamTask datastreamTask : checkpoints.keySet()) {
      _zkAdapter.setDatastreamTaskStateForKey(datastreamTask, CHECKPOINT_KEY_NAME, checkpoints.get(datastreamTask));
    }
  }

  /**
   * Read the commited checkpoints from the checkpoint store
   * @param datastreamTasks Map of the datastreamTask to the commit checkpoint for that task.
   * @return Map of the checkpoints associated with the datastream task.
   */
  @Override
  public Map<DatastreamTask, String> getCommitted(List<DatastreamTask> datastreamTasks) {
    Objects.requireNonNull(datastreamTasks, "datastreamTasks should not be null");
    return datastreamTasks.stream().collect(
        Collectors.toMap(Function.identity(), dt -> _zkAdapter.getDatastreamTaskStateForKey(dt, CHECKPOINT_KEY_NAME)));
  }
}
