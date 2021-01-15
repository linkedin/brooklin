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

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;
import static com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategyFactory.CFG_PARTITIONS_PER_TASK;
import static com.linkedin.datastream.server.assignment.StickyPartitionAssignmentStrategyFactory.CFG_PARTITION_FULLNESS_THRESHOLD_PCT;


/**
 *
 * The StickyPartitionAssignmentStrategy extends the StickyMulticastStrategy to allow performing the partition
 * assignment. This StickyPartitionAssignmentStrategy creates new tasks and remove old tasks to accommodate the
 * change in partition assignment. The strategy is also "Sticky", i.e., it minimizes the potential task mutations.
 * The total number of tasks is also unchanged during this process.
 *
 * If elastic task assignment is enabled then the initial number of tasks to create, when no tasks exist for a given
 * DatastreamGroup, is determined by a "minTasks" datastream metadata property. When the first partition assignment
 * takes place, based on the "partitionsPerTask" and "partitionFullnessFactorPct", the number of tasks needed to host
 * the partitions is determined. An in-memory map tracking the number of tasks per DatastreamGroup is updated and a
 * DatastreamRuntimeException is thrown to retry task assignment with the updated number of tasks. The next partition
 * assignment should determine that the number of tasks are sufficient and should go through successfully. After the
 * initial determination, the number of tasks does not change, task and partition assignment proceed as usual.
 *
 * When elastic task assignment is enabled, the "maxTasks" datastream metadata is used as an upper bound on the number
 * of tasks we can create to prevent unbounded task creation which can overwhelm the hosts in the cluster. This
 * semantics is different when elastic task assignment is disabled, where "maxTasks" is treated as the number of
 * tasks to create.
 */
public class StickyPartitionAssignmentStrategy extends StickyMulticastStrategy {
  public static final String CFG_MIN_TASKS = "minTasks";

  private static final Logger LOG = LoggerFactory.getLogger(StickyPartitionAssignmentStrategy.class.getName());
  private static final Integer DEFAULT_PARTITIONS_PER_TASK = 50;
  private static final Integer DEFAULT_PARTITION_FULLNESS_FACTOR_PCT = 75;

  private final boolean _enableElasticTaskAssignment;
  private final Integer _maxPartitionPerTask;
  private final Integer _partitionsPerTask;
  private final Integer _partitionFullnessFactorPct;

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
   * @param enableElasticTaskAssignment A boolean indicating whether elastic task assignment is enabled or not. If
   *                                    enabled, the number of tasks to assign will be determined by a minTasks
   *                                    datastream metadata property, and may be increased based on the number of tasks
   *                                    determined during partition assignment.
   * @param partitionsPerTask If elastic task assignment is enabled, this is used to determine the number of partitions
   *                          allowed in each task when determining the number of tasks for the first time. The default
   *                          is {@value DEFAULT_PARTITIONS_PER_TASK}.
   * @param partitionFullnessFactorPct If elastic task assignment is enabled, this is used to determine how full to
   *                                   the tasks when fitting partitions into them for the first time. The default
   *                                   is {@value DEFAULT_PARTITION_FULLNESS_FACTOR_PCT}.
   *
   */
  public StickyPartitionAssignmentStrategy(Optional<Integer> maxTasks, Optional<Integer> imbalanceThreshold,
      Optional<Integer> maxPartitionPerTask, boolean enableElasticTaskAssignment, Optional<Integer> partitionsPerTask,
      Optional<Integer> partitionFullnessFactorPct) {
    super(maxTasks, imbalanceThreshold);
    _enableElasticTaskAssignment = enableElasticTaskAssignment;
    _maxPartitionPerTask = maxPartitionPerTask.orElse(Integer.MAX_VALUE);
    _partitionsPerTask = partitionsPerTask.orElse(DEFAULT_PARTITIONS_PER_TASK);
    _partitionFullnessFactorPct = partitionFullnessFactorPct.orElse(DEFAULT_PARTITION_FULLNESS_FACTOR_PCT);

    LOG.info("Elastic task assignment is {}", _enableElasticTaskAssignment ? "enabled" : "disabled");
  }

  /**
   * Constructor for StickyPartitionAssignmentStrategy with elastic task assignment disabled
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
    this(maxTasks, imbalanceThreshold, maxPartitionPerTask, false, Optional.empty(), Optional.empty());
  }

  /**
   * Assign partitions to a particular datastream group
   *
   * @param currentAssignment the old assignment
   * @param datastreamPartitions the subscribed partitions for the particular datastream group
   * @return new assignment mapping
   */
  @Override
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

    int minTasks = resolveConfigWithMetadata(datastreamPartitions.getDatastreamGroup(), CFG_MIN_TASKS, 0);
    if (getEnableElasticTaskAssignment(minTasks) && assignedPartitions.isEmpty()) {
      // The partitions have not been assigned to any tasks yet and elastic task assignment has been enabled for this
      // datastream. Assess the number of tasks needed based on partitionsPerTask and the fullness threshold. If
      // the number of tasks needed is smaller than the number of tasks found, throw a DatastreamRuntimeException
      // so that LEADER_DO_ASSIGNMENT and LEADER_PARTITION_ASSIGNMENT can be retried with an updated number of tasks.
      int partitionsPerTask = resolveConfigWithMetadata(datastreamPartitions.getDatastreamGroup(),
          CFG_PARTITIONS_PER_TASK, _partitionsPerTask);
      int partitionFullnessFactorPct = resolveConfigWithMetadata(datastreamPartitions.getDatastreamGroup(),
          CFG_PARTITION_FULLNESS_THRESHOLD_PCT, _partitionFullnessFactorPct);
      LOG.info("Calculating number of tasks needed based on partitions per task: calculated->{}:config->{}, "
              + "fullness percentage: calculated->{}:config->{}", partitionsPerTask, _partitionsPerTask,
          partitionFullnessFactorPct, _partitionFullnessFactorPct);
      int allowedPartitionsPerTask = (partitionsPerTask * partitionFullnessFactorPct) >= 100 ?
          (partitionsPerTask * partitionFullnessFactorPct) / 100 : 1;
      int totalPartitions = datastreamPartitions.getPartitions().size();
      int numTasksNeeded = (totalPartitions / allowedPartitionsPerTask)
          + (((totalPartitions % allowedPartitionsPerTask) == 0) ? 0 : 1);
      int maxTasks = resolveConfigWithMetadata(datastreamPartitions.getDatastreamGroup(), CFG_MAX_TASKS, 0);
      if ((maxTasks > 0) && (numTasksNeeded > maxTasks)) {
        // Only have the maxTasks override kick in if it's present as part of the datastream metadata.
        LOG.warn("The number of tasks {} needed to support {} partitions per task with fullness threshold {} "
            + "is higher than maxTasks {}, setting numTasks to maxTasks", numTasksNeeded, partitionsPerTask,
            partitionFullnessFactorPct, maxTasks);
        numTasksNeeded = maxTasks;
      }
      if (numTasksNeeded > totalTaskCount) {
        _taskCountPerDatastreamGroup.put(datastreamPartitions.getDatastreamGroup(), numTasksNeeded);
        throw new DatastreamRuntimeException(
            String.format("Not enough tasks. Existing tasks: %d, tasks needed: %d, total partitions: %d",
                totalTaskCount, numTasksNeeded, totalPartitions));
      }
      LOG.info("Number of tasks needed: {}, total task count: {}", numTasksNeeded, totalTaskCount);
    }

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

    // Step 2: generate new assignment. Assign unassigned partitions to tasks and create new task if there is
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
   * Step 1) Process the targetAssignment to remove any partitions with no-op moves (partition currently assigned
   *         to the same instance where the partition is to be moved)
   * Step 2) Get the partitions that are to be moved, and find their source tasks
   * Step 3) If the instance is the one we want to move, we choose a task to which we should assign the partition
   *         from that instance
   * Step 4) Scan the current assignment, compute the new task if the old task belongs to this source task or if it
   *         is the target task we want to move the partition to
   *
   * @param currentAssignment the old assignment
   * @param targetAssignment the target assignment retrieved from Zookeeper
   * @param partitionsMetadata the subscribed partitions metadata received from connector
   * @return new assignment
   */
  @Override
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
    // map: <partitions that need to be released, source task>
    Map<String, DatastreamTaskImpl> partitionToSourceTaskMap = new HashMap<>();

    // Store the processed target assignment in a new map since targetAssignment is immutable.
    Map<String, Set<String>> processedTargetAssignment = new HashMap<>();

    // Steps 1 and 2: We first confirm that the partitions in the target assignment which can be removed, and we find
    // out its source task. If the partitions cannot be found from any task, we ignore these partitions
    currentAssignment.keySet().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);
      Set<String> partitionsAcrossAllDatastreamTasks = tasks.stream().filter(dg::belongsTo)
          .map(DatastreamTask::getPartitionsV2).flatMap(Collection::stream).collect(Collectors.toSet());

      if (targetAssignment.containsKey(instance)) {
        // Process the targetAssignment to filter out partitions that are supposed to move to the target instance that
        // they are already on to avoid unnecessary partition movements. That is, if partition1 is currently on
        // instance1 and is supposed to move to instance1, remove it from the targetAssignment as this should be a
        // no-op. Accordingly the allToReassignPartitions should also be updated to remove such partitions.
        Set<String> partitionsRemovedFromTargetAssignment = new HashSet<>(targetAssignment.get(instance));
        partitionsRemovedFromTargetAssignment.retainAll(partitionsAcrossAllDatastreamTasks);

        Set<String> updatedTargetPartitionList = new HashSet<>(targetAssignment.get(instance));
        updatedTargetPartitionList.removeAll(partitionsRemovedFromTargetAssignment);
        // allToReassignPartitions contains only valid partitions, so this step helps remove any invalid partitions on
        // the targetAssignment list.
        updatedTargetPartitionList.retainAll(allToReassignPartitions);

        processedTargetAssignment.computeIfAbsent(instance,
            val -> updatedTargetPartitionList.isEmpty() ? null : updatedTargetPartitionList);

        allToReassignPartitions.removeAll(partitionsRemovedFromTargetAssignment);
      }

      tasks.forEach(task -> {
        if (dg.belongsTo(task)) {
          Set<String> toMovePartitions = new HashSet<>(task.getPartitionsV2());
          toMovePartitions.retainAll(allToReassignPartitions);
          if (!toMovePartitions.isEmpty()) {
            // Only update the confirmedPartitionsTaskMap if a partition is indeed being deleted from it
            confirmedPartitionsTaskMap.put(task.getDatastreamTaskName(), toMovePartitions);
            toMovePartitions.forEach(p -> partitionToSourceTaskMap.put(p, (DatastreamTaskImpl) task));
          }
        }
      });
    });

    LOG.info("The processed targetAssignment: {}", processedTargetAssignment);

    Set<String> tasksToMutate = confirmedPartitionsTaskMap.keySet();
    Set<String> toReleasePartitions = new HashSet<>();
    confirmedPartitionsTaskMap.values().forEach(toReleasePartitions::addAll);

    // Compute new assignment from the current assignment
    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    currentAssignment.keySet().forEach(instance -> {
      Set<DatastreamTask> tasks = currentAssignment.get(instance);

      // check if this instance has any partition to be added
      final Set<String> toAddedPartitions = new HashSet<>();
      if (processedTargetAssignment.containsKey(instance)) {
        // filter the target assignment by the partitions which have a confirmed source
        Set<String> p = processedTargetAssignment.get(instance).stream()
            .filter(toReleasePartitions::contains).collect(Collectors.toSet());
        toAddedPartitions.addAll(p);
      }

      Set<DatastreamTask> dgTasks = tasks.stream().filter(dg::belongsTo).collect(Collectors.toSet());
      if (toAddedPartitions.size() > 0 && dgTasks.isEmpty()) {
        String errorMsg = String.format("No task is available in the target instance %s", instance);
        LOG.error(errorMsg);
        throw new DatastreamRuntimeException(errorMsg);
      }

      // Step 3: find the task with minimum number of partitions on that instance to store the moved partitions
      final DatastreamTask targetTask = toAddedPartitions.size() > 0 ? dgTasks.stream()
          .reduce((task1, task2) -> task1.getPartitionsV2().size() < task2.getPartitionsV2().size() ? task1 : task2)
          .get() : null;

      // Step 4: compute new assignment for that instance
      Set<DatastreamTask> newAssignedTask = tasks.stream().map(task -> {
        if (!dg.belongsTo(task)) {
          return task;
        }

        boolean partitionChanged = false;
        List<String> newPartitions = new ArrayList<>(task.getPartitionsV2());
        Set<DatastreamTaskImpl> extraDependencies = new HashSet<>();

        // release the partitions
        if (tasksToMutate.contains(task.getDatastreamTaskName())) {
          newPartitions.removeAll(toReleasePartitions);
          partitionChanged = true;
        }

        // add new partitions
        if (targetTask != null && task.getDatastreamTaskName().equals(targetTask.getDatastreamTaskName())) {
          newPartitions.addAll(toAddedPartitions);
          partitionChanged = true;
          // add the source task for these partitions into the extra dependency list
          toAddedPartitions.forEach(p -> extraDependencies.add(partitionToSourceTaskMap.get(p)));
        }

        if (partitionChanged) {
          DatastreamTaskImpl newTask = new DatastreamTaskImpl((DatastreamTaskImpl) task, newPartitions);
          extraDependencies.forEach(newTask::addDependency);
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
  @Override
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

  @Override
  protected int constructExpectedNumberOfTasks(DatastreamGroup dg, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignmentCopy) {
    // Count the number of tasks present for this datastream
    Set<DatastreamTask> allAliveTasks = new HashSet<>();
    for (String instance : instances) {
      List<DatastreamTask> foundDatastreamTasks =
          Optional.ofNullable(currentAssignmentCopy.get(instance)).map(c ->
              c.stream().filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()) && !allAliveTasks.contains(x))
                  .collect(Collectors.toList())).orElse(Collections.emptyList());
      allAliveTasks.addAll(foundDatastreamTasks);
    }

    int minTasks = resolveConfigWithMetadata(dg, CFG_MIN_TASKS, 0);
    boolean enableElasticTaskAssignment = getEnableElasticTaskAssignment(minTasks);
    // TODO: Fetch the number of tasks in ZK if needed
    int numTasks = enableElasticTaskAssignment ? _taskCountPerDatastreamGroup.getOrDefault(dg, 0) :
        getNumTasks(dg, instances.size());

    // Case 1: Elastic task assignment is disabled. Return getNumTasks() number of tasks.
    int expectedNumberOfTasks = numTasks;
    if (enableElasticTaskAssignment) {
      if (numTasks > 0) {
        // Case 2: elastic task enabled, numTasks > 0. This indicates that we already know how many tasks we should
        // have. Return max(numTasks, minTasks) to ensure we have at least minTasks.
        expectedNumberOfTasks = Math.max(numTasks, minTasks);
      } else {
        // Case 3: elastic task enabled, numTasks == 0. This can occur either on leader change, or if a datastream
        // is added/restarted. On leadership change, we expect to find existing tasks, whereas for new/restarted
        // datastreams we will have 0 tasks.
        //
        // In the current state, we trust the size of allAliveTasks() if present as the source of truth for the
        // expected number of tasks. This may not always be correct, and this will be corrected by persisting the
        // number of tasks to ZK when this value changes.
        expectedNumberOfTasks = Math.max(allAliveTasks.size(), minTasks);
      }
    }

    // TODO: Store the number of tasks to ZK if needed
    return expectedNumberOfTasks;
  }

  private boolean getEnableElasticTaskAssignment(int minTasks) {
    // Enable elastic assignment only if the config enables it and the datastream metadata for minTasks is present
    // and is greater than 0
    return _enableElasticTaskAssignment && (minTasks > 0);
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
