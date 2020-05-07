/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;


/**
 * The number of tasks created for datastream is configurable using "maxTasks" config. This can also be overridden at the
 * Datastream level via the Datastream metadata "maxTasks". The number of tasks is not necessarily capped at the
 * number of instances, so each instance could process multiple tasks for the same Datastream. If "maxTasks" is not
 * provided, the strategy will broadcast one task to each of the instances in the cluster.
 *
 * All the tasks are redistributed across all the instances equally.
 */
public class BroadcastStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(BroadcastStrategy.class.getName());

  private final Optional<Integer> _maxTasks;

  /**
   * Constructor for BroadcastStrategy
   * @param maxTasks Maximum number of {@link DatastreamTask}s to create out
   *                 of any {@link com.linkedin.datastream.common.Datastream}
   *                 if no value is specified for the "maxTasks" config property
   *                 at an individual datastream level.
   */
  public BroadcastStrategy(Optional<Integer> maxTasks) {
    _maxTasks = maxTasks;
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    int totalAssignedTasks = currentAssignment.values().stream().mapToInt(Set::size).sum();
    LOG.info("Assigning {} datastreams to {} instances with {} tasks", datastreams.size(), instances.size(),
        totalAssignedTasks);

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    Set<DatastreamTask> tasksAvailableToReuse = new HashSet<>();
    // Make a copy of the current assignment, since the strategy modifies it during calculation
    Map<String, Set<DatastreamTask>> currentAssignmentCopy = new HashMap<>(currentAssignment.size());
    currentAssignment.forEach((k, v) -> {
      currentAssignmentCopy.put(k, new HashSet<>(v));
      // The service comes up as a new instance node on every restart with a new live instance number.
      // In this strategy, we will reuse the task from the dead instances rather than creating a new task, to avoid
      // extra zookeeper write operations to delete the unused task and create a new task.

      // A leader can get interrupted while doing the dead instance cleanup during leader assignment because of shutdown
      // sequence. There is a make before break logic in the code. So, if the leader got interrupted before cleaning up
      // the dead instance and the task is already assigned to another live-instance, the next leader must avoid
      // reassigning the same task. So, building the full list and then removing the reused tasks rather than building the
      // list from dead tasks is important. Otherwise, two live-instances can get the same task because of reuse logic
      // resulting in updateAllAssignments to fail.
      tasksAvailableToReuse.addAll(v);
    });

    for (String instance : instances) {
      newAssignment.put(instance, new HashSet<>());
      currentAssignmentCopy.computeIfAbsent(instance, i -> new HashSet<>());
      Set<DatastreamTask> instanceTasks = currentAssignmentCopy.computeIfAbsent(instance, i -> new HashSet<>());
      tasksAvailableToReuse.removeAll(instanceTasks);
    }

    Map<String, List<DatastreamTask>> reuseTaskMap =
        tasksAvailableToReuse.stream().collect(Collectors.groupingBy(DatastreamTask::getTaskPrefix));
    int instancePos = 0;
    for (DatastreamGroup dg : datastreams) {
      List<DatastreamTask> reuseTasksPerDg = reuseTaskMap.getOrDefault(dg.getTaskPrefix(), Collections.emptyList());

      int numTasks = getNumTasks(dg, instances.size());
      for (int taskPos = 0; taskPos < numTasks; taskPos++) {
        String instance = instances.get(instancePos);

        DatastreamTask foundDatastreamTask = currentAssignmentCopy.get(instance)
            .stream()
            .filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()))
            .findFirst()
            .orElseGet(() -> (!reuseTasksPerDg.isEmpty()) ? reuseTasksPerDg.remove(reuseTasksPerDg.size() - 1) :
              new DatastreamTaskImpl(dg.getDatastreams()));

        currentAssignmentCopy.get(instance).remove(foundDatastreamTask);
        newAssignment.get(instance).add(foundDatastreamTask);

        // Move to the next instance
        instancePos = (instancePos + 1) % instances.size();
      }
    }

    LOG.info("New assignment is {}", newAssignment);

    return newAssignment;
  }

  private int getNumTasks(DatastreamGroup dg, int numInstances) {
    // Look for an override in any of the datastream. In the case of multiple overrides, select the largest.
    // If no override is present then use the default "_maxTasks" from config.
    return dg.getDatastreams()
        .stream()
        .map(ds -> ds.getMetadata().get(CFG_MAX_TASKS))
        .filter(Objects::nonNull)
        .mapToInt(Integer::valueOf)
        .filter(x -> x > 0)
        .max()
        .orElse(_maxTasks.orElse(numInstances));
  }
}
