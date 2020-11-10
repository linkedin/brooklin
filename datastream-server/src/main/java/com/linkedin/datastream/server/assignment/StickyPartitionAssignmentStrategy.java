/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamGroup;
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
   *                            If partitions count in task is larger than this number, Brooklin will throw an exception
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

    Validate.isTrue(currentAssignment.size() > 0,
        "Zero tasks assigned. Retry leader partition assignment.");

    String dgName = datastreamPartitions.getDatastreamGroup().getName();

    // Step 1: collect the # of tasks and figured out the unassigned partitions
    List<String> assignedPartitions = new ArrayList<>();
    int totalTaskCount = 0;
    for (Set<DatastreamTask> tasks : currentAssignment.values()) {
      Set<DatastreamTask> dgTask = tasks.stream().filter(t -> dgName.equals(t.getTaskPrefix())).collect(Collectors.toSet());
      dgTask.forEach(t -> assignedPartitions.addAll(t.getPartitionsV2()));
      totalTaskCount += dgTask.size();
    }

    Validate.isTrue(totalTaskCount > 0, String.format("No tasks found for datastream group %s", dgName));

    List<String> unassignedPartitions = new ArrayList<>(datastreamPartitions.getPartitions());
    unassignedPartitions.removeAll(assignedPartitions);

    int maxPartitionPerTask = datastreamPartitions.getPartitions().size() / totalTaskCount;

    // calculate how many tasks are allowed to have slightly more partitions
    // Assume we have total N tasks, the maxPartitionsPerTask (ceiling) is k. R is the remainder.
    // We will have R tasks with k partitions and (N-R) task with k -1 partitions.
    // The code is written in a way a task will be iterate once so we need to knows
    // if this task belongs to R(with k partitions) or (N-R) with (k-1) partitions.

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
   * Move a partition for a datastream group according to the targetAssignment. As we are only allowed to mutate the
   * task once. It follow the steps
   * Step 1) get the partitions that to be moved, and find out their source task
   * Step 2) If the instance is the one we want to move, we find out the task which we should assign the partition
   *         from that instance
   * Step 3) Scan the current assignment, compute new task if the old task belongs to these source tasks or if it
   *         is the target task we want to move to
   *
   * @param currentAssignment the old assignment
   * @param targetAssignment the target assignment retrieved from Zookeeper
   * @param partitionsMetadata the subscribed partitions metadata received from connector
   * @return new assignment
   */
  public Map<String, Set<DatastreamTask>> movePartitions(Map<String, Set<DatastreamTask>> currentAssignment,
      Map<String, Set<String>> targetAssignment, DatastreamGroupPartitionsMetadata partitionsMetadata) {

    LOG.info("Move partition, current assignment: {}, target assignment: {}, all partitions: {}", currentAssignment,
        targetAssignment, partitionsMetadata.getPartitions());

    DatastreamGroup dg = partitionsMetadata.getDatastreamGroup();

    Set<String> allToReassignPartitions = new HashSet<>();
    targetAssignment.values().forEach(allToReassignPartitions::addAll);
    allToReassignPartitions.retainAll(partitionsMetadata.getPartitions());

    // construct a map to store the tasks and if it contain the partitions that can be released
    // map: <source taskName, partitions that need to be released>
    Map<String, Set<String>> confirmedPartitionsTaskMap = new HashMap<>();

    // construct a map to store the partition and its source task
    // map: <partitions that need to be released, source taskName>
    Map<String, String> partitionToSourceTaskMap = new HashMap<>();

    // We first confirm that the partitions in the target assignment which can be removed, and we find out its source task
    // If the partitions cannot be found from any task, we ignore these partitions
    currentAssignment.keySet().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);
      tasks.forEach(task -> {
        if (dg.belongsTo(task)) {
          Set<String> toMovePartitions = new HashSet<>(task.getPartitionsV2());
          toMovePartitions.retainAll(allToReassignPartitions);
          confirmedPartitionsTaskMap.put(task.getDatastreamTaskName(), toMovePartitions);
          toMovePartitions.forEach(p -> partitionToSourceTaskMap.put(p, task.getDatastreamTaskName()));
        }
      });
    });

    Set<String> tasksToMutate = confirmedPartitionsTaskMap.keySet();
    Set<String> toReleasePartitions = new HashSet<>();
    confirmedPartitionsTaskMap.values().forEach(v -> toReleasePartitions.addAll(v));

    // Compute new assignment from the current assignment
    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    currentAssignment.keySet().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);

      // check if this instance has any partition to be added
      final Set<String> toAddedPartitions = new HashSet<>();
      if (targetAssignment.containsKey(instance)) {
        // filter the target assignment by the partitions which have a confirmed source
        Set<String> p = targetAssignment.get(instance).stream()
            .filter(toReleasePartitions::contains).collect(Collectors.toSet());
        toAddedPartitions.addAll(p);
      }

      Set<DatastreamTask> dgTasks = tasks.stream().filter(dg::belongsTo)
          .collect(Collectors.toSet());
      if (toAddedPartitions.size() > 0 && dgTasks.isEmpty()) {
        String errorMsg = String.format("No task is available in the target instance %s", instance);
        LOG.error(errorMsg);
        throw new DatastreamRuntimeException(errorMsg);
      }

      // find the task with minimum number of partitions on that instance to store the moved partitions
      final DatastreamTask targetTask = toAddedPartitions.size() > 0 ? dgTasks.stream()
          .reduce((task1, task2) -> task1.getPartitionsV2().size() < task2.getPartitionsV2().size() ? task1 : task2)
          .get() : null;

        // compute new assignment for that instance
        Set<DatastreamTask> newAssignedTask = tasks.stream().map(task -> {
          if (!dg.belongsTo(task)) {
            return task;
          }
          boolean partitionChanged = false;
          List<String> newPartitions = new ArrayList<>(task.getPartitionsV2());
          Set<String> extraDependencies = new HashSet<>();

          // release the partitions
          if (tasksToMutate.contains(task.getDatastreamTaskName())) {
            newPartitions.removeAll(toReleasePartitions);
            partitionChanged = true;
          }

          // add new partitions
          if (targetTask != null && task.getDatastreamTaskName().equals(targetTask.getDatastreamTaskName())) {
            newPartitions.addAll(toAddedPartitions);
            partitionChanged = true;
            // add source task for these partitions into extra dependency
            toReleasePartitions.forEach(p -> extraDependencies.add(partitionToSourceTaskMap.get(p)));
          }

          if (partitionChanged) {
            DatastreamTaskImpl newTask = new DatastreamTaskImpl((DatastreamTaskImpl) task, newPartitions);
            extraDependencies.forEach(t -> newTask.addDependency(t));
            return newTask;
          } else {
            return task;
          }
        }).collect(Collectors.toSet());
      newAssignment.put(instance, newAssignedTask);
    });

    LOG.info("assignment info, task: {}", newAssignment);
    partitionSanityChecks(newAssignment, partitionsMetadata);

    return newAssignment;
  }

  /**
   * This method checks the current assignment and returns the list of tasks which are in the
   * dependency list as well as in current assignment. The logic is the task in the dependency list
   * must not be present in the current assignment list. It's possible when the previous leader was
   * not able to complete the update on the zookeeper and the new leader gets the intermediate state
   * from the zookeeper.
   *
   * @param datastreamGroups datastream groups to associate the tasks with
   * @param currentAssignment existing assignment
   * @return  list of datastream tasks mapped by instance that need to be cleaned up.
   */
  public Map<String, List<DatastreamTask>> getTasksToCleanUp(List<DatastreamGroup> datastreamGroups,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    Set<String> datastreamGroupsSet = datastreamGroups.stream().map(DatastreamGroup::getTaskPrefix).collect(Collectors.toSet());
    Map<String, List<DatastreamTask>> tasksToCleanUp = new HashMap<>();
    // map of task name to DatastreamTask for future reference
    Map<String, DatastreamTask> assignmentsMap = currentAssignment.values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toMap(DatastreamTask::getDatastreamTaskName, Function.identity()));

    for (String instance : currentAssignment.keySet()) {
      // find the dependency tasks which also exist in the assignmentsMap.
      List<DatastreamTask> dependencyTasksPerInstance = currentAssignment.get(instance)
          .stream()
          .filter(t -> datastreamGroupsSet.contains(t.getTaskPrefix()))
          .map(task -> ((DatastreamTaskImpl) task).getDependencies())
          .flatMap(Collection::stream)
          .map(assignmentsMap::get)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());

      if (!dependencyTasksPerInstance.isEmpty()) {
        tasksToCleanUp.put(instance, dependencyTasksPerInstance);
      }
    }
    return tasksToCleanUp;
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
      String errorMsg = String.format("Validation failed after assignment, assigned partitions "
          + "size: %s is not equal to all partitions size: %s", total, allPartitions.getPartitions().size());
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
