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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;

/**
 *
 * The StickyPartitionAssignmentStrategy extends the StickyMulticastStrategy but allows to perform the partition
 * assignment. This StickyPartitionAssignmentStrategy creates new tasks and remove old tasks to accommodate the
 * change in partition assignment. The strategy is also "Sticky", i.e., it minimizes the potential task mutations.
 * The total number of tasks is also unchanged during this process.
 */
public class StickyPartitionAssignmentStrategy extends StickyMulticastStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());
  private final Integer _maxPartitionPerTask;

  /**
   * Constructor for StickyPartitionAssignmentStrategy
   * @param maxTasks Maximum number of {@link DatastreamTask}s to create out
   *                 of any {@link com.linkedin.datastream.common.Datastream}
   *                 if no value is specified for the "maxTasks" config property
   *                 at an individual datastream level.
   * @param imbalanceThreshold The maximum allowable difference in the number of tasks assigned
   *                           between any two {@link com.linkedin.datastream.server.Coordinator}
   *                           instances, before triggering a rebalance. The default is
   *                           {@value DEFAULT_IMBALANCE_THRESHOLD}.
   * @param maxPartitionPerTask The maximum number of partitions allowed per task. By default it's Integer.MAX (no limit)
   *                     If partitions count in task is larger than this number, Brooklin will throw an exception
   *
   */
  public StickyPartitionAssignmentStrategy(Optional<Integer> maxTasks, Optional<Integer> imbalanceThreshold,
      Optional<Integer> maxPartitionPerTask) {
    super(maxTasks, imbalanceThreshold);
    _maxPartitionPerTask = maxPartitionPerTask.orElse(Integer.MAX_VALUE);
  }
  /**
   * assign partitions to a particular datastream group
   *
   * @param currentAssignment the old assignment
   * @param datastreamPartitions the subscribed partitions for the particular datastream group
   * @return new assignment mapping
   */
  public Map<String, Set<DatastreamTask>> assignPartitions(Map<String,
      Set<DatastreamTask>> currentAssignment, DatastreamGroupPartitionsMetadata datastreamPartitions) {

    LOG.info("old partition assignment info, assignment: {}", currentAssignment);

    String dgName = datastreamPartitions.getDatastreamGroup().getName();

    // Step 1: collect the # of tasks and figured out the unassigned partitions
    List<String> assignedPartitions = new ArrayList<>();
    int totalTaskCount = 0;
    for (Set<DatastreamTask> tasks : currentAssignment.values()) {
      Set<DatastreamTask> dgTask = tasks.stream().filter(t -> dgName.equals(t.getTaskPrefix())).collect(Collectors.toSet());
      dgTask.stream().forEach(t -> assignedPartitions.addAll(t.getPartitionsV2()));
      totalTaskCount += dgTask.size();
    }

    List<String> unassignedPartitions = new ArrayList<>(datastreamPartitions.getPartitions());
    unassignedPartitions.removeAll(assignedPartitions);

    int maxPartitionPerTask = datastreamPartitions.getPartitions().size() / totalTaskCount;
    // calculate how many tasks are allowed to have slightly more partitions
    final AtomicInteger remainder = new AtomicInteger(datastreamPartitions.getPartitions().size() % totalTaskCount);
    LOG.debug("maxPartitionPerTask {}, task count {}", maxPartitionPerTask, totalTaskCount);

    Collections.shuffle(unassignedPartitions);

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    //Step 2: generate new assignment. Assign unassigned partitions to tasks and create new task if there is
    // a partition change
    currentAssignment.keySet().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);
      Set<DatastreamTask> newAssignedTask = tasks.stream().map(task -> {
        if (!dgName.equals(task.getTaskPrefix())) {
          return task;
        } else {
          Set<String> newPartitions = new HashSet<>(task.getPartitionsV2());
          newPartitions.retainAll(datastreamPartitions.getPartitions());

          //We need to create new task if the partition is changed
          boolean partitionChanged = newPartitions.size() != task.getPartitionsV2().size();

          int allowedPartitions = remainder.get() > 0 ? maxPartitionPerTask + 1 : maxPartitionPerTask;

          while (newPartitions.size() < allowedPartitions && unassignedPartitions.size() > 0) {
            newPartitions.add(unassignedPartitions.remove(unassignedPartitions.size() - 1));
            partitionChanged = true;
          }

          if (remainder.get() > 0) {
            remainder.decrementAndGet();
          }

          if (newPartitions.size() > _maxPartitionPerTask) {
            String errorMessage = String.format("Partition count %s is larger than %s for datastream %s, "
                + "please increase the maxTask", newPartitions.size(), _maxPartitionPerTask, dgName);
            throw new DatastreamRuntimeException(errorMessage);
          }
          if (partitionChanged) {
            return new DatastreamTaskImpl((DatastreamTaskImpl) task, newPartitions);
          } else {
            return task;
          }
        }
      }).collect(Collectors.toSet());
      newAssignment.put(instance, newAssignedTask);
    });
    LOG.info("new assignment info, assignment: {}, all partitions: {}", newAssignment,
        datastreamPartitions.getPartitions());

    partitionSanityChecks(newAssignment, datastreamPartitions);
    return newAssignment;
  }

  /**
   * check if the computed assignment contains all the partitions
   */
  private void partitionSanityChecks(Map<String, Set<DatastreamTask>> assignedTasks,
      DatastreamGroupPartitionsMetadata allPartitions) {
    int total = 0;

    List<String> unassignedPartitions = new ArrayList<>(allPartitions.getPartitions());
    String datastreamGroupName = allPartitions.getDatastreamGroup().getName();
    for (Set<DatastreamTask> tasksSet : assignedTasks.values()) {
      for (DatastreamTask task : tasksSet) {
        if (datastreamGroupName.equals(task.getTaskPrefix())) {
          total += task.getPartitionsV2().size();
          unassignedPartitions.removeAll(task.getPartitionsV2());
        }
      }
    }
    if (total != allPartitions.getPartitions().size()) {
      String errorMsg = String.format(String.format("Validation failed after assignment, assigned partitions "
          + "size: %s is not equal to all partitions size: %s", total, allPartitions.getPartitions().size()));
      LOG.error(errorMsg);
      throw new DatastreamRuntimeException(errorMsg);
    }
    if (unassignedPartitions.size() > 0) {
      String errorMsg = String.format("Validation failed after assignment, "
          + "unassigned partition: %s", unassignedPartitions);
      LOG.error(errorMsg);
      throw new DatastreamRuntimeException(errorMsg);
    }
  }
}
