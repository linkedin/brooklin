/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamPartitionsMetadata;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;

/**
 * An partition assignment strategy. This StickyPartitionAssignmentStrategy creates new tasks and remove old tasks
 * to accommodate the change in partition assignment. The total number of tasks is unchanged during this process.
 * The strategy is also "Sticky", i.e., it minimize the potential partitions change between new tasks/old tasks
 */
public class StickyPartitionAssignmentStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());

  /**
   * assign partitions to a particular datastream group
   *
   * @param currentAssignment the old assignment
   * @param allPartitions the subscribed partitions received from partition listener
   * @return new assignment mapping
   */
  public Map<String, Set<DatastreamTask>> assignPartitions(Map<String,
      Set<DatastreamTask>> currentAssignment, DatastreamPartitionsMetadata allPartitions) {

    LOG.info("old partition assignment info, assignment: {}", currentAssignment);

    String dgName = allPartitions.getDatastreamGroupName();

    List<String> assignedPartitions = new ArrayList<>();
    int totalTaskCount = 0;
    for (Set<DatastreamTask> tasks : currentAssignment.values()) {
      Set<DatastreamTask> dgTask = tasks.stream().filter(t -> dgName.equals(t.getTaskPrefix())).collect(Collectors.toSet());
      dgTask.stream().forEach(t -> assignedPartitions.addAll(t.getPartitionsV2()));
      totalTaskCount += dgTask.size();
    }

    List<String> unassignedPartitions = new ArrayList<>(allPartitions.getPartitions());
    unassignedPartitions.removeAll(assignedPartitions);

    int maxPartitionPerTask = allPartitions.getPartitions().size() / totalTaskCount;
    final AtomicInteger remainder = new AtomicInteger(allPartitions.getPartitions().size() % totalTaskCount);
    LOG.info("maxPartitionPerTask {}, task count {}", maxPartitionPerTask, totalTaskCount);

    Collections.shuffle(unassignedPartitions);

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    currentAssignment.keySet().stream().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);
      Set<DatastreamTask> newAssignedTask = tasks.stream().map(task -> {
        if (!dgName.equals(task.getTaskPrefix())) {
          return task;
        } else {
          Set<String> partitions = new HashSet<>(task.getPartitionsV2());
          partitions.retainAll(allPartitions.getPartitions());

          //We need to create new task if the partition is changed
          boolean partitionChanged = partitions.size() != task.getPartitionsV2().size();

          int allowedPartitions = remainder.get() > 0 ? maxPartitionPerTask + 1 : maxPartitionPerTask;

          while (partitions.size() < allowedPartitions && unassignedPartitions.size() > 0) {
            partitions.add(unassignedPartitions.remove(unassignedPartitions.size() - 1));
            partitionChanged = true;
          }

          if (remainder.get() > 0) {
            remainder.decrementAndGet();
          }

          if (partitionChanged) {
            return new DatastreamTaskImpl((DatastreamTaskImpl) task, partitions);
          } else {
            return task;
          }
        }
      }).collect(Collectors.toSet());
      newAssignment.put(instance, newAssignedTask);
    });
    LOG.info("new assignment info, assignment: {}, all partitions: {}", newAssignment, allPartitions);

    sanityChecks(newAssignment, allPartitions);
    return newAssignment;
  }

  /**
   * check if the computed assignment have all the partitions
   */
  private void sanityChecks(Map<String, Set<DatastreamTask>> assignedTasks, DatastreamPartitionsMetadata allPartitions) {
    int total = 0;

    List<String> unassignedPartitions = new ArrayList<>(allPartitions.getPartitions());
    String datastreamGroupName = allPartitions.getDatastreamGroupName();
    for (Set<DatastreamTask> tasksSet : assignedTasks.values()) {
      for (DatastreamTask task : tasksSet) {
        if (datastreamGroupName.equals(task.getTaskPrefix())) {
          total += task.getPartitionsV2().size();
          unassignedPartitions.removeAll(task.getPartitionsV2());
        }
      }
    }
    if (total != allPartitions.getPartitions().size()) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, assigned partitions "
          + "size: {} is not equal to all partitions size: {}", total, allPartitions.getPartitions().size()));
    }
    if (unassignedPartitions.size() > 0) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment, "
          + "unassigned partition: {}", unassignedPartitions));
    }
  }
}
