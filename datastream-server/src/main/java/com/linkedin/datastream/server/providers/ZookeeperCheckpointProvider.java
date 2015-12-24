package com.linkedin.datastream.server.providers;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.zk.ZkAdapter;


public class ZookeeperCheckpointProvider implements CheckpointProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCheckpointProvider.class.getName());

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
    LOG.info("Commit called with checkpoints " + checkpoints.toString());
    Validate.notNull(checkpoints, "Checkpoints should not be null");
    for (DatastreamTask datastreamTask : checkpoints.keySet()) {
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
    Validate.notNull(datastreamTasks, "datastreamTasks should not be null");
    Map<DatastreamTask, String> checkpoints = new HashMap<>();
    for (DatastreamTask task : datastreamTasks) {
      String checkpoint = _zkAdapter.getDatastreamTaskStateForKey(task, CHECKPOINT_KEY_NAME);
      if (checkpoint != null) {
        checkpoints.put(task, checkpoint);
      } else {
        LOG.debug("Checkpoint doesn't exist for DatastreamTask " + task.toString());
      }
    }

    LOG.info("GetCommitted returning the last committed checkpoints " + checkpoints.toString());
    return checkpoints;
  }
}
