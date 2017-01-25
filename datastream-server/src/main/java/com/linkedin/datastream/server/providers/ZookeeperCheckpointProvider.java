package com.linkedin.datastream.server.providers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.Validate;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.zk.ZkAdapter;


public class ZookeeperCheckpointProvider implements CheckpointProvider {

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCheckpointProvider.class.getName());
  private static final String CHECKPOINT_KEY_NAME = "sourceCheckpoint";

  private final ZkAdapter _zkAdapter;

  private static final String NUM_CHECKPOINT_COMMITS = "numCheckpointCommits";
  private static final String CHECKPOINT_COMMIT_LATENCY_MS = "checkpointCommitLatencyMs";
  private final DynamicMetricsManager _dynamicMetricsManager;

  private Map<DatastreamTask, Map<Integer, String>> _checkpointsToCommit = new HashMap<>();
  private Map<DatastreamTask, Instant> _lastCommitTime = new HashMap<>();
  private static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(1);

  // Instruct jackson to convert string keys to integer
  TypeReference<ConcurrentHashMap<Integer, String>> _hashMapTypeReference =
      new TypeReference<ConcurrentHashMap<Integer, String>>() {
      };

  public ZookeeperCheckpointProvider(ZkAdapter zkAdapter) {
    _zkAdapter = zkAdapter;
    // Initialize metrics
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
  }

  /**
   * Commit the checkpoints to the checkpoint store.
   */
  @Override
  public synchronized void updateCheckpoint(DatastreamTask task, int partition, String checkpoint) {
    if (!_checkpointsToCommit.containsKey(task)) {
      _checkpointsToCommit.put(task, new HashMap<>());
    }
    _checkpointsToCommit.get(task).put(partition, checkpoint);

    if (!_lastCommitTime.containsKey(task) || Instant.now()
        .isAfter(_lastCommitTime.get(task).plus(CHECKPOINT_INTERVAL))) {
      writeCheckpointsToStore(task);
    }
  }

  private void writeCheckpointsToStore(DatastreamTask task) {
    long startTime = System.currentTimeMillis();
    Map<Integer, String> checkpoints = mergeAndGetSafeCheckpoints(task);

    _zkAdapter.setDatastreamTaskStateForKey(task, CHECKPOINT_KEY_NAME, JsonUtils.toJson(checkpoints));
    _dynamicMetricsManager.createOrUpdateMeter(this.getClass(), NUM_CHECKPOINT_COMMITS, 1);
    _dynamicMetricsManager.createOrUpdateHistogram(this.getClass(), CHECKPOINT_COMMIT_LATENCY_MS,
        System.currentTimeMillis() - startTime);
    // Clear the checkpoints to commit.
    _checkpointsToCommit.get(task).clear();
    _lastCommitTime.put(task, Instant.now());
  }

  @Override
  public synchronized void flush() {
    _checkpointsToCommit.keySet().forEach(this::writeCheckpointsToStore);
  }

  private Map<Integer, String> mergeAndGetSafeCheckpoints(DatastreamTask task) {

    Map<Integer, String> safeCheckpoints = _checkpointsToCommit.get(task);

    // It is possible that the safe checkpoints contains only subset of partitions.
    // So it is safe to merge them with the existing checkpoints in the zookeeper.
    Map<Integer, String> checkpoints = getCheckpoint(task);

    if (safeCheckpoints != null && !safeCheckpoints.isEmpty()) {
      for (Map.Entry<Integer, String> safeCheckpoint : safeCheckpoints.entrySet()) {
        checkpoints.put(safeCheckpoint.getKey(), safeCheckpoint.getValue());
      }
    }

    return checkpoints;
  }

  /**
   * Get the safe checkpoints that the task has produced to. It is possible that the checkpoint provider is not
   * writing the checkpoints every time updateCheckpoint is called in which case safe checkpoints will return checkpoints
   * from the memory.
   */
  @Override
  public synchronized Map<Integer, String> getSafeCheckpoints(DatastreamTask task) {
    return mergeAndGetSafeCheckpoints(task);
  }

  private Map<Integer, String> getCheckpoint(DatastreamTask task) {
    String checkpoint = _zkAdapter.getDatastreamTaskStateForKey(task, CHECKPOINT_KEY_NAME);
    if (checkpoint != null) {
      return JsonUtils.fromJson(checkpoint, _hashMapTypeReference);
    } else {
      LOG.info("Checkpoint doesn't exist for DatastreamTask " + task.toString());
      return new HashMap<>();
    }
  }

  /**
   * Read the checkpoints from the checkpoint store for the task
   * @param datastreamTask datastream tasks whose checkpoints need to be read
   * @return Map of the checkpoints associated with the datastream task.
   */
  @Override
  public Map<Integer, String> getCommitted(DatastreamTask datastreamTask) {
    Validate.notNull(datastreamTask, "datastreamTask should not be null");
    Map<Integer, String> checkpoints = getCheckpoint(datastreamTask);
    LOG.info("GetCommitted returning the last committed checkpoints " + checkpoints.toString());
    return checkpoints;
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();

    metrics.add(new BrooklinMeterInfo(buildMetricName(NUM_CHECKPOINT_COMMITS)));
    metrics.add(new BrooklinHistogramInfo(buildMetricName(CHECKPOINT_COMMIT_LATENCY_MS)));

    return Collections.unmodifiableList(metrics);
  }
}
