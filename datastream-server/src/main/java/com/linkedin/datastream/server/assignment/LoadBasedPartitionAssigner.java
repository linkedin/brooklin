/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.ClusterThroughputInfo;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.PartitionThroughputInfo;


/**
 * Performs partition assignment based on partition throughput information
 */
public class LoadBasedPartitionAssigner {
  /**
   * Performs partition assignment based on partition throughput information.
   * <p>
   * Unlike other assignment algorithms, this one can result in uneven distribution of partitions in tasks. Partitions
   * with no throughput information (such as newly discovered partitions) will be assigned to tasks using round-robin.
   * </p>
   * @param throughputInfo Per partition throughput information
   * @param currentAssignment Current assignment
   * @param unassignedPartitions Unassigned partitions
   * @param partitionMetadata Partition metadata
   * @return New assignment
   */
  public Map<String, Set<DatastreamTask>> assignPartitions(ClusterThroughputInfo throughputInfo,
      Map<String, Set<DatastreamTask>> currentAssignment, List<String> unassignedPartitions,
      DatastreamGroupPartitionsMetadata partitionMetadata, int maxPartitionsPerTask) {
    String datastreamGroupName = partitionMetadata.getDatastreamGroup().getName();
    Map<String, PartitionThroughputInfo> partitionInfoMap = throughputInfo.getPartitionInfoMap();

    // filter out all the tasks for the current datastream group, and retain assignments in a map
    Map<String, Set<String>> newPartitions = new HashMap<>();
    currentAssignment.values().forEach(tasks ->
        tasks.forEach(task -> {
          if (task.getTaskPrefix().equals(datastreamGroupName)) {
            Set<String> retainedPartitions = new HashSet<>(task.getPartitionsV2());
            retainedPartitions.retainAll(partitionMetadata.getPartitions());
            newPartitions.put(task.getId(), retainedPartitions);
          }
    }));

    int numPartitions = newPartitions.values().stream().mapToInt(Set::size).sum();
    numPartitions += unassignedPartitions.size();
    int numTasks = newPartitions.size();
    validatePartitionCountAndThrow(numTasks, numPartitions, maxPartitionsPerTask);

    // sort the current assignment's tasks on total throughput
    Map<String, Integer> taskThroughputMap = new HashMap<>();
    PartitionThroughputInfo defaultPartitionInfo = new PartitionThroughputInfo(
        PartitionAssignmentStrategyConfig.PARTITION_BYTES_IN_KB_RATE_DEFAULT,
        PartitionAssignmentStrategyConfig.PARTITION_MESSAGES_IN_RATE_DEFAULT, "");
    newPartitions.forEach((task, partitions) -> {
      int totalThroughput = partitions.stream()
          .mapToInt(p -> partitionInfoMap.getOrDefault(p, defaultPartitionInfo).getBytesInKBRate())
          .sum();
      taskThroughputMap.put(task, totalThroughput);
    });

    ArrayList<String> recognizedPartitions = new ArrayList<>(); // partitions with throughput info
    ArrayList<String> unrecognizedPartitions = new ArrayList<>(); // partitions without throughput info
    for (String partition : unassignedPartitions) {
      if (partitionInfoMap.containsKey(partition)) {
        recognizedPartitions.add(partition);
      } else {
        unrecognizedPartitions.add(partition);
      }
    }

    // sort unassigned partitions with throughput info on throughput
    recognizedPartitions.sort((p1, p2) -> {
      Integer p1KBRate = partitionInfoMap.get(p1).getBytesInKBRate();
      Integer p2KBRate = partitionInfoMap.get(p2).getBytesInKBRate();
      return p1KBRate.compareTo(p2KBRate);
    });

    // build a priority queue of tasks based on throughput
    // only add tasks that can accommodate more partitions in the queue
    List<String> tasks = newPartitions.keySet().stream()
        .filter(t -> newPartitions.get(t).size() < maxPartitionsPerTask)
        .collect(Collectors.toList());
    PriorityQueue<String> taskQueue = new PriorityQueue<>(Comparator.comparing(taskThroughputMap::get));
    taskQueue.addAll(tasks);

    // assign partitions with throughput info one by one, by putting the heaviest partition in the lightest task
    while (recognizedPartitions.size() > 0 && taskQueue.size() > 0) {
       String heaviestPartition = recognizedPartitions.remove(recognizedPartitions.size() - 1);
       int heaviestPartitionThroughput = partitionInfoMap.get(heaviestPartition).getBytesInKBRate();
       String lightestTask = taskQueue.poll();
       newPartitions.get(lightestTask).add(heaviestPartition);
       taskThroughputMap.put(lightestTask, taskThroughputMap.get(lightestTask) + heaviestPartitionThroughput);
       int currentNumPartitions = newPartitions.get(lightestTask).size();
       // don't put the task back in the queue if the number of its partitions is maxed out
       if (currentNumPartitions < maxPartitionsPerTask) {
         taskQueue.add(lightestTask);
       }
    }

    // assign unrecognized partitions with round-robin
    int index = 0;
    for (String partition : unrecognizedPartitions) {
      index = findTaskWithRoomForAPartition(tasks, newPartitions, index, maxPartitionsPerTask);
      String currentTask = tasks.get(index);
      newPartitions.get(currentTask).add(partition);
      index = (index + 1) % tasks.size();
    }

    // build the new assignment using the new partitions for the affected datastream's tasks
    Map<String, Set<DatastreamTask>> newAssignments = new HashMap<>();
    currentAssignment.keySet().forEach(instance -> {
      Set<DatastreamTask> oldTasks = currentAssignment.get(instance);
      Set<DatastreamTask> newTasks = oldTasks.stream()
          .map(task -> {
            if (task.getTaskPrefix().equals(datastreamGroupName)) {
              return new DatastreamTaskImpl((DatastreamTaskImpl) task, newPartitions.get(task.getId()));
            }
        return task;
      }).collect(Collectors.toSet());
      newAssignments.put(instance, newTasks);
    });

    return newAssignments;
  }

  private void validatePartitionCountAndThrow(int numTasks, int numPartitions, int maxPartitionsPerTask) {
    // conversion to long to avoid integer overflow
    if (numTasks * (long) maxPartitionsPerTask < numPartitions) {
      String message = String.format("Not enough tasks to fit partitions. Number of tasks: %d, " +
          "number of partitions: %d, max partitions per task: %d", numTasks, numPartitions, maxPartitionsPerTask);
      throw new DatastreamRuntimeException(message);
    }
  }

  @VisibleForTesting
  int findTaskWithRoomForAPartition(List<String> tasks, Map<String, Set<String>> partitionMap, int startIndex,
      int maxPartitionsPerTask) {
    for (int i = 0; i < tasks.size(); i++) {
      int currentIndex = (startIndex + i) % tasks.size();
      String currentTask = tasks.get(currentIndex);
      if (partitionMap.get(currentTask).size() < maxPartitionsPerTask) {
        return currentIndex;
      }
    }
    return -1;
  }
}
