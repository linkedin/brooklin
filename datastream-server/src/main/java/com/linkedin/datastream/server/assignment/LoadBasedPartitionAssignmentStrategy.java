package com.linkedin.datastream.server.assignment;

import java.util.Optional;

import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.PartitionThroughputProvider;


public class LoadBasedPartitionAssignmentStrategy extends StickyPartitionAssignmentStrategy {
  private final PartitionThroughputProvider _throughputProvider;

  public LoadBasedPartitionAssignmentStrategy(PartitionThroughputProvider throughputProvider, Optional<Integer> maxTasks,
      Optional<Integer> imbalanceThreshold, Optional<Integer> maxPartitionPerTask, boolean enableElasticTaskAssignment,
      Optional<Integer> partitionsPerTask, Optional<Integer> partitionFullnessFactorPct, Optional<ZkClient> zkClient,
      String clusterName) {
    super(maxTasks, imbalanceThreshold, maxPartitionPerTask, enableElasticTaskAssignment, partitionsPerTask,
        partitionFullnessFactorPct, zkClient, clusterName);

    _throughputProvider = throughputProvider;
  }


}
