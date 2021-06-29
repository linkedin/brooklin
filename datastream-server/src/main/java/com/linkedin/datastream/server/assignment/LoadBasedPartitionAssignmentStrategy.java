/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

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
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
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
  private static final String CLASS_NAME = LoadBasedPartitionAssignmentStrategy.class.getSimpleName();
  private static final DynamicMetricsManager DYNAMIC_METRICS_MANAGER = DynamicMetricsManager.getInstance();
  private static final String THROUGHPUT_INFO_FETCH_RATE = "throughputInfoFetchRate";

  private final PartitionThroughputProvider _throughputProvider;
  private final int _taskCapacityMBps;
  private final int _taskCapacityUtilizationPct;
  private final int _throughputInfoFetchTimeoutMs;
  private final int _throughputInfoFetchRetryPeriodMs;
  private final LoadBasedPartitionAssigner _assigner;

  // TODO Make these configurable
  private final boolean _enableThroughputBasedPartitionAssignment = true;
  private final boolean _enablePartitionNumBasedTaskCountEstimation = true;

  /**
   * Creates an instance of {@link LoadBasedPartitionAssignmentStrategy}
   */
  public LoadBasedPartitionAssignmentStrategy(PartitionThroughputProvider throughputProvider, Optional<Integer> maxTasks,
      int imbalanceThreshold, int maxPartitionPerTask, boolean enableElasticTaskAssignment, int partitionsPerTask,
      int partitionFullnessFactorPct, int taskCapacityMBps, int taskCapacityUtilizationPct,
      int throughputInfoFetchTimeoutMs, int throughputInfoFetchRetryPeriodMs, ZkClient zkClient, String clusterName) {
    super(maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, zkClient, clusterName);
    _throughputProvider = throughputProvider;
    _taskCapacityMBps = taskCapacityMBps;
    _taskCapacityUtilizationPct = taskCapacityUtilizationPct;
    _throughputInfoFetchTimeoutMs = throughputInfoFetchTimeoutMs;
    _throughputInfoFetchRetryPeriodMs = throughputInfoFetchRetryPeriodMs;
    _assigner = new LoadBasedPartitionAssigner();
  }

  /**
   * {@inheritDoc}
   */
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
    Map<String, Set<DatastreamTask>> assignment = _assigner.assignPartitions(
        clusterThroughputInfo, currentAssignment, unassignedPartitions, datastreamPartitions, _maxPartitionPerTask);
    LOG.info("New assignment info, assignment: {}", assignment);
    return assignment;
  }

  private ClusterThroughputInfo fetchPartitionThroughputInfo(DatastreamGroup datastreamGroup) {
    AtomicInteger attemptNum = new AtomicInteger(0);
    return PollUtils.poll(() -> {
      try {
        DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(CLASS_NAME, THROUGHPUT_INFO_FETCH_RATE, 1);
        return _throughputProvider.getThroughputInfo(datastreamGroup);
      } catch (Exception ex) {
        attemptNum.set(attemptNum.get() + 1);
        LOG.warn(String.format("Failed to fetch partition throughput info on attempt %d", attemptNum.get()), ex);
        return null;
      }
    }, Objects::nonNull, _throughputInfoFetchRetryPeriodMs, _throughputInfoFetchTimeoutMs)
        .orElseThrow(RetriesExhaustedException::new);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metricInfos = new ArrayList<>();
    metricInfos.add(new BrooklinMeterInfo(CLASS_NAME + "." + THROUGHPUT_INFO_FETCH_RATE));
    metricInfos.addAll(_assigner.getMetricInfos());
    return Collections.unmodifiableList(metricInfos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cleanupStrategy() {
    _assigner.cleanupMetrics();
    super.cleanupStrategy();
  }

  @Override
  protected void unregisterMetrics(String datastream) {
    _assigner.unregisterMetricsForDatastream(datastream);
  }
}
