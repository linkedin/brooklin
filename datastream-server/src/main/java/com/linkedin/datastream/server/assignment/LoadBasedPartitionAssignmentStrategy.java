/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.linkedin.datastream.server.DatastreamSourceClusterResolver;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.providers.PartitionThroughputProvider;


/**
 * Partition assignment strategy that does assignment based on throughput information supplied by a
 * {@link PartitionThroughputProvider}
 */
public class LoadBasedPartitionAssignmentStrategy extends StickyPartitionAssignmentStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedPartitionAssignmentStrategy.class.getName());

  // TODO Make these constants configurable
  private static final long THROUGHPUT_INFO_FETCH_TIMEOUT_MS_DEFAULT = Duration.ofSeconds(10).toMillis();
  private static final long THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS_DEFAULT = Duration.ofSeconds(1).toMillis();

  private final PartitionThroughputProvider _throughputProvider;
  private final DatastreamSourceClusterResolver _sourceClusterResolver;


  /**
   * Creates an instance of {@link LoadBasedPartitionAssignmentStrategy}
   */
  public LoadBasedPartitionAssignmentStrategy(PartitionThroughputProvider throughputProvider,
      DatastreamSourceClusterResolver sourceClusterResolver, Optional<Integer> maxTasks,
      Optional<Integer> imbalanceThreshold, Optional<Integer> maxPartitionPerTask, boolean enableElasticTaskAssignment,
      Optional<Integer> partitionsPerTask, Optional<Integer> partitionFullnessFactorPct, Optional<ZkClient> zkClient,
      String clusterName) {
    super(maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, zkClient, clusterName);
    _throughputProvider = throughputProvider;
    _sourceClusterResolver = sourceClusterResolver;
  }

  @Override
  public Map<String, Set<DatastreamTask>> assignPartitions(Map<String, Set<DatastreamTask>> currentAssignment,
      DatastreamGroupPartitionsMetadata datastreamPartitions) {
    DatastreamGroup datastreamGroup = datastreamPartitions.getDatastreamGroup();
    String datastreamGroupName = datastreamGroup.getName();
    HashMap<String, ClusterThroughputInfo> partitionThroughputInfo;

    // Attempting to retrieve partition throughput info with a fallback mechanism to StickyPartitionAssignmentStrategy
    try {
      partitionThroughputInfo = fetchPartitionThroughputInfo();
    } catch (RetriesExhaustedException ex) {
      LOG.warn("Attempts to fetch partition throughput timed out. Falling back to regular partition assignment strategy");
      return super.assignPartitions(currentAssignment, datastreamPartitions);
    }

    LOG.info("Old partition assignment info, assignment: {}", currentAssignment);
    Validate.isTrue(currentAssignment.size() > 0,
        "Zero tasks assigned. Retry leader partition assignment.");

    // Calculating task count and assigned partitions
    int taskCount = getTaskCountForDatastreamGroup(currentAssignment, datastreamGroupName);
    List<String> assignedPartitions = getAssignedPartitionsForDatastreamGroup(currentAssignment, datastreamGroupName);

    // Elastic task count validation
    if (getEnableElasticTaskAssignment(datastreamGroup)) {
      if (assignedPartitions.isEmpty()) {
        performElasticTaskCountValidation(datastreamPartitions, taskCount);
      }
      updateOrRegisterElasticTaskAssignmentMetrics(datastreamPartitions, taskCount);
    }

    // Calculating unassigned partitions
    List<String> unassignedPartitions = new ArrayList<>(datastreamPartitions.getPartitions());
    unassignedPartitions.removeAll(assignedPartitions);

    // Getting a task count estimate based on throughput
    String clusterName = _sourceClusterResolver.getSourceCluster(datastreamPartitions.getDatastreamGroup());
    LoadBasedTaskCountEstimator taskCountEstimator = new LoadBasedTaskCountEstimator();
    ClusterThroughputInfo clusterThroughputInfo = partitionThroughputInfo.get(clusterName);
    int throughputTaskCountEstimate = taskCountEstimator.getTaskCount(clusterThroughputInfo, assignedPartitions,
        unassignedPartitions);

    // Picking the maximum task count between elastic task count and throughput based task count estimate
    int maxTaskCount = Math.max(taskCount, throughputTaskCountEstimate);

    // Doing assignment
    LoadBasedPartitionAssigner partitionAssigner = new LoadBasedPartitionAssigner();
    return partitionAssigner.assignPartitions(clusterThroughputInfo, assignedPartitions, unassignedPartitions,
        maxTaskCount);
  }

  private HashMap<String, ClusterThroughputInfo> fetchPartitionThroughputInfo() {
    PollUtils.poll(() -> {
      try {
        return _throughputProvider.getThroughputInfo();
      } catch (Exception ex) {
        LOG.warn("Failed to fetch partition throughput info.");
        return null;
      }
    }, Objects::nonNull, THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS_DEFAULT, THROUGHPUT_INFO_FETCH_TIMEOUT_MS_DEFAULT)
        .orElseThrow(RetriesExhaustedException::new);

    return null;
  }
}
