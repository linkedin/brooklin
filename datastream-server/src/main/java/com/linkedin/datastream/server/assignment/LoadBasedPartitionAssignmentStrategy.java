/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.RetriesExhaustedException;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.Pair;
import com.linkedin.datastream.server.providers.PartitionThroughputProvider;


/**
 * Partition assignment strategy that does assignment based on throughput information supplied by a
 * {@link PartitionThroughputProvider}
 */
public class LoadBasedPartitionAssignmentStrategy extends StickyPartitionAssignmentStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedPartitionAssignmentStrategy.class.getName());

  private static final int THROUGHPUT_INFO_FETCH_TIMEOUT_MS_DEFAULT = (int) Duration.ofSeconds(10).toMillis();
  private static final int THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS_DEFAULT = (int) Duration.ofSeconds(1).toMillis();

  private static final int TASK_CAPACITY_MBPS_DEFAULT = 4;
  private static final int TASK_CAPACITY_UTILIZATION_PCT_DEFAULT = 90;

  private final PartitionThroughputProvider _throughputProvider;
  private final int _taskCapacityMBps;
  private final int _taskCapacityUtilizationPct;
  private final int _throughputInfoFetchTimeoutMs;
  private final int _throughputInfoFetchRetryPeriodMs;

  // TODO Make this a config
  private final boolean _enableThroughputBasedTaskCountEstimation = true;


  /**
   * Creates an instance of {@link LoadBasedPartitionAssignmentStrategy}
   */
  public LoadBasedPartitionAssignmentStrategy(PartitionThroughputProvider throughputProvider,
      Optional<Integer> maxTasks, Optional<Integer> imbalanceThreshold, Optional<Integer> maxPartitionPerTask,
      boolean enableElasticTaskAssignment, Optional<Integer> partitionsPerTask,
      Optional<Integer> partitionFullnessFactorPct, Optional<Integer> taskCapacityMBps,
      Optional<Integer> taskCapacityUtilizationPct, Optional<Integer> throughputInfoFetchTimeoutMs,
      Optional<Integer> throughputInfoFetchRetryPeriodMs, Optional<ZkClient> zkClient,
      String clusterName) {
    super(maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, zkClient, clusterName);
    _throughputProvider = throughputProvider;
    _taskCapacityMBps = taskCapacityMBps.orElse(TASK_CAPACITY_MBPS_DEFAULT);
    _taskCapacityUtilizationPct = taskCapacityUtilizationPct.orElse(TASK_CAPACITY_UTILIZATION_PCT_DEFAULT);
    _throughputInfoFetchTimeoutMs = throughputInfoFetchTimeoutMs.orElse(THROUGHPUT_INFO_FETCH_TIMEOUT_MS_DEFAULT);
    _throughputInfoFetchRetryPeriodMs = throughputInfoFetchRetryPeriodMs.
        orElse(THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS_DEFAULT);
  }

  @Override
  public Map<String, Set<DatastreamTask>> assignPartitions(Map<String, Set<DatastreamTask>> currentAssignment,
      DatastreamGroupPartitionsMetadata datastreamPartitions) {
    DatastreamGroup datastreamGroup = datastreamPartitions.getDatastreamGroup();
    if (!isElasticTaskAssignmentEnabled(datastreamGroup)) {
      LOG.info("Elastic task assignment not enabled. Falling back to StickyPartitionAssignmentStrategy");
      return super.assignPartitions(currentAssignment, datastreamPartitions);
    }

    String datastreamGroupName = datastreamGroup.getName();
    Pair<List<String>, Integer> assignedPartitionsAndTaskCount = getAssignedPartitionsAndTaskCountForDatastreamGroup(
        currentAssignment, datastreamGroupName);
    List<String> assignedPartitions = assignedPartitionsAndTaskCount.getKey();
    int taskCount = assignedPartitionsAndTaskCount.getValue();

    ClusterThroughputInfo clusterThroughputInfo = null;
    // Attempting to retrieve partition throughput info with a fallback mechanism to StickyPartitionAssignmentStrategy
    try {
      clusterThroughputInfo = fetchPartitionThroughputInfo(datastreamGroup);
    } catch (RetriesExhaustedException ex) {
      LOG.warn("Attempts to fetch partition throughput timed out");
      if (assignedPartitions.isEmpty()) {
        LOG.info("Throughput information unavailable during initial assignment. Falling back to StickyPartitionAssignmentStrategy");
        return super.assignPartitions(currentAssignment, datastreamPartitions);
      }
    }

    LOG.info("Old partition assignment info, assignment: {}", currentAssignment);
    Validate.notNull(clusterThroughputInfo, "Cluster throughput info must not be null");
    Validate.isTrue(currentAssignment.size() > 0,
        "Zero tasks assigned. Retry leader partition assignment");

    int numTasksNeeded = taskCount;

    // Calculating unassigned partitions
    List<String> unassignedPartitions = new ArrayList<>(datastreamPartitions.getPartitions());
    unassignedPartitions.removeAll(assignedPartitions);

    // TODO Figure out if validation is needed to prevent double assignment
    // TODO Figure out how to integrate with existing metrics
    // TODO Figure out if new metrics are needed

    // Estimating number of tasks needed
    if (isPartitionNumBasedTaskCountEstimationEnabled(datastreamGroup)) {
      numTasksNeeded = getTaskCountEstimateBasedOnNumPartitions(datastreamPartitions, taskCount);
    }

    if (isThroughputBasedTaskCountEstimationEnabled()) {
      LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(_taskCapacityMBps, _taskCapacityUtilizationPct);
      numTasksNeeded = Math.max(numTasksNeeded, estimator.getTaskCount(clusterThroughputInfo, assignedPartitions,
          unassignedPartitions));
    }

    if (numTasksNeeded > taskCount) {
      updateNumTasksAndForceTaskCreation(datastreamPartitions, numTasksNeeded, taskCount);
    }

    // Doing assignment
    LoadBasedPartitionAssigner partitionAssigner = new LoadBasedPartitionAssigner();
    return partitionAssigner.assignPartitions(clusterThroughputInfo, currentAssignment,
        unassignedPartitions, datastreamPartitions);
  }

  private ClusterThroughputInfo fetchPartitionThroughputInfo(DatastreamGroup datastreamGroup) {
    return PollUtils.poll(() -> {
      try {
        return _throughputProvider.getThroughputInfo(datastreamGroup);
      } catch (Exception ex) {
        // TODO print exception and retry count
        LOG.warn("Failed to fetch partition throughput info.");
        return null;
      }
    }, Objects::nonNull, _throughputInfoFetchRetryPeriodMs, _throughputInfoFetchTimeoutMs)
        .orElseThrow(RetriesExhaustedException::new);
  }

  private boolean isThroughputBasedTaskCountEstimationEnabled() {
    return _enableThroughputBasedTaskCountEstimation;
  }
}
