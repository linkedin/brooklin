/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;

import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.zk.ZkAdapter;

/**
 * ZooKeeper-backed {@link CheckpointProvider} that maintains {@link DatastreamTask}
 * processing state information, e.g. offsets/checkpoints, errors.
 */
public class ZookeeperCheckpointProvider implements CheckpointProvider {

  public static final String CHECKPOINT_KEY_NAME = "sourceCheckpoint";

  private static final Logger LOG = LoggerFactory.getLogger(ZookeeperCheckpointProvider.class.getName());
  private static final String MODULE = ZookeeperCheckpointProvider.class.getSimpleName();
  private static final String NUM_CHECKPOINT_COMMITS = "numCheckpointCommits";
  private static final String CHECKPOINT_COMMIT_LATENCY_MS = "checkpointCommitLatencyMs";
  private static final Duration CHECKPOINT_INTERVAL = Duration.ofMinutes(1);

  private final ZkAdapter _zkAdapter;
  private final DynamicMetricsManager _dynamicMetricsManager;

  // Instruct jackson to convert string keys to integer
  final TypeReference<ConcurrentHashMap<Integer, String>> _hashMapTypeReference =
      new TypeReference<ConcurrentHashMap<Integer, String>>() {
      };

  private final ConcurrentHashMap<DatastreamTask, Map<Integer, String>> _checkpoints = new ConcurrentHashMap<>();
  private final Set<DatastreamTask> _checkpointsToCommit = new HashSet<>();
  private final ConcurrentHashMap<DatastreamTask, Instant> _lastCommitTime = new ConcurrentHashMap<>();

  /**
   * Construct an instance of ZookeeperCheckpointProvider
   * @param zkAdapter ZooKeeper client adapter to use
   */
  public ZookeeperCheckpointProvider(ZkAdapter zkAdapter) {
    _zkAdapter = zkAdapter;
    // Initialize metrics
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
  }

  @Override
  public void unassignDatastreamTask(DatastreamTask task) {
    _checkpoints.remove(task);
    synchronized (_checkpointsToCommit) {
      _checkpointsToCommit.remove(task);
    }
    _lastCommitTime.remove(task);
  }

  /**
   * Commit the checkpoints to the checkpoint store.
   */
  @Override
  public void updateCheckpoint(DatastreamTask task, int partition, String checkpoint) {
    Map<Integer, String> taskMap = getOrAddCheckpointMap(task);
    synchronized (taskMap) {
      taskMap.put(partition, checkpoint);

      if (!_lastCommitTime.containsKey(task) || Instant.now()
          .isAfter(_lastCommitTime.get(task).plus(CHECKPOINT_INTERVAL))) {
        writeCheckpointsToStore(task);
        synchronized (_checkpointsToCommit) {
          _checkpointsToCommit.remove(task);
        }
      } else {
        synchronized (_checkpointsToCommit) {
          _checkpointsToCommit.add(task);
        }
      }
    }
  }

  private Map<Integer, String> getOrAddCheckpointMap(DatastreamTask task) {
    return _checkpoints.computeIfAbsent(task, k -> new HashMap<>());
  }

  private void writeCheckpointsToStore(DatastreamTask task) {
    Map<Integer, String> taskMap = getOrAddCheckpointMap(task);
    synchronized (taskMap) {
      long startTime = System.currentTimeMillis();
      Map<Integer, String> checkpoints = mergeAndGetSafeCheckpoints(task, taskMap);

      _zkAdapter.setDatastreamTaskStateForKey(task, CHECKPOINT_KEY_NAME, JsonUtils.toJson(checkpoints));
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, NUM_CHECKPOINT_COMMITS, 1);
      _dynamicMetricsManager.createOrUpdateHistogram(MODULE, CHECKPOINT_COMMIT_LATENCY_MS,
          System.currentTimeMillis() - startTime);

      Map<Integer, String> committedCheckpoints = _checkpoints.get(task);
      // This check is necessary since task may have been unassigned/removed by a concurrent call to unassignDatastreamTask().
      if (committedCheckpoints != null) {
        // Clear the checkpoints to commit.
        committedCheckpoints.clear();
      }
      _lastCommitTime.put(task, Instant.now());
    }
  }

  @Override
  public void flush() {
    synchronized (_checkpointsToCommit) {
      LOG.info("Flushing checkpoints for {} datatstream tasks to ZooKeeper", _checkpointsToCommit.size());
      _checkpointsToCommit.forEach(this::writeCheckpointsToStore);
      _checkpointsToCommit.clear();
    }
    LOG.info("Flushing checkpoints to ZooKeeper completed successfully");
  }

  private Map<Integer, String> mergeAndGetSafeCheckpoints(DatastreamTask task, Map<Integer, String> safeCheckpoints) {

    // It is possible that the safe checkpoints contains only subset of partitions.
    // So it is safe to merge them with the existing checkpoints in ZooKeeper.
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
  public Map<Integer, String> getSafeCheckpoints(DatastreamTask task) {
    Map<Integer, String> taskMap = getOrAddCheckpointMap(task);
    synchronized (taskMap) {
      return mergeAndGetSafeCheckpoints(task, taskMap);
    }
  }

  private Map<Integer, String> getCheckpoint(DatastreamTask task) {
    String checkpoint = _zkAdapter.getDatastreamTaskStateForKey(task, CHECKPOINT_KEY_NAME);
    if (StringUtils.isNotBlank(checkpoint)) {
      return JsonUtils.fromJson(checkpoint, _hashMapTypeReference);
    } else {
      LOG.info("Checkpoint doesn't exist for DatastreamTask " + task.toString());
      return new HashMap<>();
    }
  }

  /**
   * Read the checkpoints from the checkpoint store for the task
   * @param datastreamTask datastream task whose checkpoints need to be read
   * @return Map of the checkpoints associated with {@code datastreamTask}
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