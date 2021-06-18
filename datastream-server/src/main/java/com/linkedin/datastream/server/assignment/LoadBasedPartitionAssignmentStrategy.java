/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

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

  // TODO Make these configurable
  private final boolean _enableThroughputBasedPartitionAssignment = true;
  private final boolean _enablePartitionNumBasedTaskCountEstimation = true;

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

    // For throughput based partition-assignment to kick in, the following conditions must be met:
    //   (1) Elastic task assignment must be enabled through configuration
    //   (2) Throughput-based task assignment must be enabled through configuration
    boolean enableElasticTaskAssignment = isElasticTaskAssignmentEnabled(datastreamGroup);
    if (!enableElasticTaskAssignment || !_enableThroughputBasedPartitionAssignment) {
      LOG.info("Throughput based elastic task assignment not enabled. Falling back to sticky partition assignment.");
      LOG.info("enableElasticTaskAssignment: {}, enableThroughputBasedPartitionAssignment {}",
          enableElasticTaskAssignment, _enableThroughputBasedPartitionAssignment);
      return super.assignPartitions(currentAssignment, datastreamPartitions);
    }

    String datastreamGroupName = datastreamGroup.getName();
    Pair<List<String>, Integer> assignedPartitionsAndTaskCount = getAssignedPartitionsAndTaskCountForDatastreamGroup(
        currentAssignment, datastreamGroupName);
    List<String> assignedPartitions = assignedPartitionsAndTaskCount.getKey();
    int taskCount = assignedPartitionsAndTaskCount.getValue();
    LOG.info("Old partition assignment info, assignment: {}", currentAssignment);
    Validate.isTrue(taskCount > 0, String.format("No tasks found for datastream group %s", datastreamGroup));
    Validate.isTrue(currentAssignment.size() > 0,
        "Zero tasks assigned. Retry leader partition assignment");

    // Calculating unassigned partitions
    List<String> unassignedPartitions = new ArrayList<>(datastreamPartitions.getPartitions());
    unassignedPartitions.removeAll(assignedPartitions);

    ClusterThroughputInfo clusterThroughputInfo = new ClusterThroughputInfo(StringUtils.EMPTY, Collections.emptyMap());
    if (assignedPartitions.isEmpty()) {
      try {
        // Attempting to retrieve partition throughput info on initial assignment
        clusterThroughputInfo = fetchPartitionThroughputInfo(datastreamGroup);
      } catch (RetriesExhaustedException ex) {
        LOG.warn("Attempts to fetch partition throughput timed out");
        LOG.info("Throughput information unavailable during initial assignment. Falling back to sticky partition assignment");
        return super.assignPartitions(currentAssignment, datastreamPartitions);
      }

      // Task count update happens only on initial assignment (when datastream makes the STOPPED -> READY transition).
      // The calculation is based on the maximum of:
      //   (1) Tasks already allocated for the datastream
      //   (2) Partition number based estimate, if the appropriate config is enabled
      //   (3) Throughput based task count estimate
      int numTasksNeeded = taskCount;
      if (_enablePartitionNumBasedTaskCountEstimation) {
        numTasksNeeded = getTaskCountEstimateBasedOnNumPartitions(datastreamPartitions, taskCount);
      }

      LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(_taskCapacityMBps, _taskCapacityUtilizationPct);
      numTasksNeeded = Math.max(numTasksNeeded, estimator.getTaskCount(clusterThroughputInfo, assignedPartitions,
          unassignedPartitions));

      // Task count is validated against max tasks config
      numTasksNeeded = validateNumTasksAgainstMaxTasks(datastreamPartitions, numTasksNeeded);
      if (numTasksNeeded > taskCount) {
        updateNumTasksAndForceTaskCreation(datastreamPartitions, numTasksNeeded, taskCount);
      }
    }

    // TODO Implement metrics
    // Doing assignment
    Map<String, Set<DatastreamTask>> newAssignment = doAssignment(clusterThroughputInfo, currentAssignment,
        unassignedPartitions, datastreamPartitions);
    partitionSanityChecks(newAssignment, datastreamPartitions);
    return newAssignment;
  }

  @VisibleForTesting
  Map<String, Set<DatastreamTask>> doAssignment(ClusterThroughputInfo clusterThroughputInfo,
      Map<String, Set<DatastreamTask>> currentAssignment, List<String> unassignedPartitions,
      DatastreamGroupPartitionsMetadata datastreamPartitions) {
    LoadBasedPartitionAssigner partitionAssigner = new LoadBasedPartitionAssigner();
    Map<String, Set<DatastreamTask>> assignment = partitionAssigner.assignPartitions(clusterThroughputInfo,
        currentAssignment, unassignedPartitions, datastreamPartitions, _maxPartitionPerTask);
    LOG.info("new assignment info, assignment: {}", assignment);
    return assignment;
  }

  private ClusterThroughputInfo fetchPartitionThroughputInfo(DatastreamGroup datastreamGroup) {
    AtomicInteger attemptNum = new AtomicInteger(0);
    return PollUtils.poll(() -> {
      try {
        return _throughputProvider.getThroughputInfo(datastreamGroup);
      } catch (Exception ex) {
        attemptNum.set(attemptNum.get() + 1);
        LOG.warn(String.format("Failed to fetch partition throughput info on attempt %d", attemptNum.get()), ex);
        return null;
      }
    }, Objects::nonNull, _throughputInfoFetchRetryPeriodMs, _throughputInfoFetchTimeoutMs)
        .orElseThrow(RetriesExhaustedException::new);
  }
}
