package com.linkedin.datastream.server.providers;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;

import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.zk.ZkAdapter;


public class ZookeeperCheckpointProvider implements CheckpointProvider {
  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCheckpointProvider.class.getName());
  private static final String CHECKPOINT_KEY_NAME = "sourceCheckpoint";

  private final ZkAdapter _zkAdapter;

  private final Meter _numCheckpointCommits;
  private final Histogram _checkpointCommitLatency;

  public ZookeeperCheckpointProvider(ZkAdapter zkAdapter) {
    _zkAdapter = zkAdapter;

    // Initialize metrics
    _numCheckpointCommits = new Meter();
    _checkpointCommitLatency = new Histogram(new ExponentiallyDecayingReservoir());
  }

  /**
   * Commit the checkpoints to the checkpoint store.
   * @param checkpoints Map of the datastreamTask to the checkpoint associated with the datastreamTask
   */
  @Override
  public void commit(Map<DatastreamTask, String> checkpoints) {
    LOG.info("Commit called with checkpoints " + checkpoints.toString());
    Validate.notNull(checkpoints, "Checkpoints should not be null");
    long startTime = System.currentTimeMillis();
    checkpoints.forEach((key, value) -> {
      _zkAdapter.setDatastreamTaskStateForKey(key, CHECKPOINT_KEY_NAME, value);
    });
    _numCheckpointCommits.mark(checkpoints.size());
    _checkpointCommitLatency.update(System.currentTimeMillis() - startTime);
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

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();

    metrics.put(buildMetricName("numCheckpointCommits"), _numCheckpointCommits);
    metrics.put(buildMetricName("checkpointCommitLatency"), _checkpointCommitLatency);

    return Collections.unmodifiableMap(metrics);
  }
}
