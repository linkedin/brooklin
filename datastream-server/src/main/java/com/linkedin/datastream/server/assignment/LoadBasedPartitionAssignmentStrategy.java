package com.linkedin.datastream.server.assignment;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import java.util.stream.Collectors;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.PartitionThroughputInfo;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.RetriesExhaustedException;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.PartitionThroughputProvider;


/**
 * Partition assignment strategy that does assignment based on throughput information supplied by a
 * {@link PartitionThroughputProvider}
 */
public class LoadBasedPartitionAssignmentStrategy extends StickyPartitionAssignmentStrategy {
  private final PartitionThroughputProvider _throughputProvider;

  private static final Logger LOG = LoggerFactory.getLogger(LoadBasedPartitionAssignmentStrategy.class.getName());
  private static final long THROUGHPUT_INFO_FETCH_TIMEOUT_MS_DEFAULT = Duration.ofSeconds(5).toMillis();

  /**
   * Creates an instance of {@link LoadBasedPartitionAssignmentStrategy}
   */
  public LoadBasedPartitionAssignmentStrategy(PartitionThroughputProvider throughputProvider, Optional<Integer> maxTasks,
      Optional<Integer> imbalanceThreshold, Optional<Integer> maxPartitionPerTask, boolean enableElasticTaskAssignment,
      Optional<Integer> partitionsPerTask, Optional<Integer> partitionFullnessFactorPct, Optional<ZkClient> zkClient,
      String clusterName) {
    super(maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, zkClient, clusterName);

    _throughputProvider = throughputProvider;
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {
    return super.assign(datastreams, instances, currentAssignment);
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

    // TODO: Do partition throughput based task count estimation, then pick the max

    // Calculating unassigned partitions
    List<String> unassignedPartitions = new ArrayList<>(datastreamPartitions.getPartitions());
    unassignedPartitions.removeAll(assignedPartitions);

    // Sorting topic partitions by throughput
    String clusterName = getClusterNameFromDatastreamGroup(datastreamPartitions.getDatastreamGroup());
    ClusterThroughputInfo clusterThroughputInfo = partitionThroughputInfo.get(clusterName);
    HashMap<String, PartitionThroughputInfo> partitionInfoMap = clusterThroughputInfo.getPartitionInfoMap();
    List<PartitionThroughputInfo> sortedPartitions = partitionInfoMap.values().stream().
        sorted((PartitionThroughputInfo o1, PartitionThroughputInfo o2) -> {
          if (o1.getBytesInRate() != o2.getBytesInRate()) {
            return o1.getBytesInRate() - o2.getBytesInRate();
          } else {
            return o1.getMessagesInRate() - o2.getMessagesInRate();
          }
        }).collect(Collectors.toList());



    return null;
  }

  private String getClusterNameFromDatastreamGroup(DatastreamGroup datastreamGroup) {
    // TODO: Figure out a clean way to do this
    return "metrics";
  }

  private HashMap<String, ClusterThroughputInfo> fetchPartitionThroughputInfo() {
    PollUtils.poll(() -> {
      try {
        return _throughputProvider.getThroughputInfo();
      } catch (Exception ex) {
        LOG.warn("Failed to fetch partition throughput info.");
        return null;
      }
    }, Objects::nonNull,100, THROUGHPUT_INFO_FETCH_TIMEOUT_MS_DEFAULT)
        .orElseThrow(RetriesExhaustedException::new);

    return null;
  }
}
