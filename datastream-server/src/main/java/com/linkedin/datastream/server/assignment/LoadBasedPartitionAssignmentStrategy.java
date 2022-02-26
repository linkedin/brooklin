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

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;


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
  private final boolean _enableThroughputBasedPartitionAssignment;
  private final boolean _enablePartitionNumBasedTaskCountEstimation;
  private final LoadBasedPartitionAssigner _assigner;
  private final int _defaultPartitionBytesInKBRate;
  private final int _defaultPartitionMsgsInRate;

  /**
   * Creates an instance of {@link LoadBasedPartitionAssignmentStrategy}
   */
  public LoadBasedPartitionAssignmentStrategy(PartitionThroughputProvider throughputProvider, Optional<Integer> maxTasks,
      int imbalanceThreshold, int maxPartitionPerTask, boolean enableElasticTaskAssignment, int partitionsPerTask,
      int partitionFullnessFactorPct, int taskCapacityMBps, int taskCapacityUtilizationPct,
      int throughputInfoFetchTimeoutMs, int throughputInfoFetchRetryPeriodMs, ZkClient zkClient, String clusterName,
      boolean enableThroughputBasedPartitionAssignment, boolean enablePartitionNumBasedTaskCountEstimation,
      int defaultPartitionBytesInKBRate, int defaultPartitionMsgsInRate) {
    super(maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, zkClient, clusterName);
    _throughputProvider = throughputProvider;
    _taskCapacityMBps = taskCapacityMBps;
    _taskCapacityUtilizationPct = taskCapacityUtilizationPct;
    _throughputInfoFetchTimeoutMs = throughputInfoFetchTimeoutMs;
    _throughputInfoFetchRetryPeriodMs = throughputInfoFetchRetryPeriodMs;
    _enableThroughputBasedPartitionAssignment = enableThroughputBasedPartitionAssignment;
    _enablePartitionNumBasedTaskCountEstimation = enablePartitionNumBasedTaskCountEstimation;
    _defaultPartitionBytesInKBRate = defaultPartitionBytesInKBRate;
    _defaultPartitionMsgsInRate = defaultPartitionMsgsInRate;

    LOG.info("Task capacity : {}MBps, task capacity utilization : {}%, Throughput info fetch timeout : {} ms, "
        + "throughput info fetch retry period : {} ms, throughput based partition assignment : {}, "
        + "partition num based task count estimation : {}", _taskCapacityMBps, _taskCapacityUtilizationPct,
        _throughputInfoFetchTimeoutMs, _throughputInfoFetchRetryPeriodMs, _enableThroughputBasedPartitionAssignment ?
            "enabled" : "disabled", _enablePartitionNumBasedTaskCountEstimation ? "enabled" : "disabled");
    _assigner = new LoadBasedPartitionAssigner(defaultPartitionBytesInKBRate, defaultPartitionMsgsInRate);
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
      int numTasksEstimateBasedOnPartitionCount = 0;
      int numTasksNeeded = taskCount;
      if (_enablePartitionNumBasedTaskCountEstimation) {
        numTasksEstimateBasedOnPartitionCount = getTaskCountEstimateBasedOnNumPartitions(datastreamPartitions, taskCount);
        numTasksNeeded = numTasksEstimateBasedOnPartitionCount;
      }

      LoadBasedTaskCountEstimator estimator = new LoadBasedTaskCountEstimator(_taskCapacityMBps, _taskCapacityUtilizationPct,
          _defaultPartitionBytesInKBRate, _defaultPartitionMsgsInRate);
      int numTasksEstimateBasedOnLoad = estimator.getTaskCount(clusterThroughputInfo, assignedPartitions,
          unassignedPartitions, datastreamGroupName);
      numTasksNeeded = Math.max(numTasksNeeded, numTasksEstimateBasedOnLoad);

      LOG.info("NumTask estimations for datastream {}: existingTasks: {}, PartitionCountBasedEstimate: {} "
              + "LoadBasedEstimate: {}. Final numTasks: {}", datastreamGroupName, taskCount,
          numTasksEstimateBasedOnPartitionCount, numTasksEstimateBasedOnLoad, numTasksNeeded);

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

    boolean needsAdjustment = false;
    // verify if the elastic task configurations need adjustment for the datastream.
    int maxTasks = resolveConfigWithMetadata(datastreamPartitions.getDatastreamGroup(), CFG_MAX_TASKS, 0);
    // if numTasks == maxTasks, the task configurations require readjustment from scale point of view.
    if (maxTasks > 0 && maxTasks == getTaskCountForDatastreamGroup(datastreamGroup.getTaskPrefix())) {
      needsAdjustment = true;
    }

    if (_enablePartitionNumBasedTaskCountEstimation) {
      // if the partition count does not honor the partitionsPerTask configuration, the elastic task configurations
      // require readjustment.
      int partitionsPerTask = getPartitionsPerTask(datastreamPartitions.getDatastreamGroup());
      for (Set<DatastreamTask> tasksSet : newAssignment.values()) {
        for (DatastreamTask task : tasksSet) {
          if (task.getTaskPrefix().equals(datastreamGroup.getTaskPrefix()) && task.getPartitionsV2().size() > partitionsPerTask) {
            needsAdjustment = true;
            break;
          }
        }
      }
    }
    updateOrRegisterElasticTaskAssignmentMetrics(datastreamGroup.getTaskPrefix(), needsAdjustment);
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
    List<BrooklinMetricInfo> baseStrategyMetricInfos = super.getMetricInfos();
    List<BrooklinMetricInfo> metricInfos = new ArrayList<>(baseStrategyMetricInfos);
    metricInfos.add(new BrooklinMeterInfo(CLASS_NAME + "." + THROUGHPUT_INFO_FETCH_RATE));
    metricInfos.addAll(_assigner.getMetricInfos());
    metricInfos.addAll(_throughputProvider.getMetricInfos());
    return Collections.unmodifiableList(metricInfos);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void cleanupStrategy() {
    super.cleanupStrategy();
  }

  @Override
  protected void unregisterMetrics(String datastream) {
    _assigner.unregisterMetricsForDatastream(datastream);
  }
}
