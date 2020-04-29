/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

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
      // building the full list and then removing the reused tasks rather than building the list from dead tasks
      // to avoid assignment of same task from dead instance to two live instances in case of interrupt exception.
      tasksAvailableToReuse.addAll(v);
    });

    for (String instance : instances) {
      newAssignment.put(instance, new HashSet<>());
      currentAssignmentCopy.putIfAbsent(instance, new HashSet<>());
      tasksAvailableToReuse.removeAll(currentAssignmentCopy.get(instance));
    }

    int instancePos = 0;
    for (DatastreamGroup dg : datastreams) {
      List<DatastreamTask> reuseTasksPerDg = tasksAvailableToReuse.stream().filter(x ->
          x.getTaskPrefix().equals(dg.getTaskPrefix())).collect(Collectors.toList());

      int numTasks = getNumTasks(dg, instances.size());
      for (int taskPos = 0; taskPos < numTasks; taskPos++) {
        String instance = instances.get(instancePos);

        DatastreamTask foundDatastreamTask = currentAssignmentCopy.get(instance)
            .stream()
            .filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()))
            .findFirst()
            .orElse(null);
        if (foundDatastreamTask == null) {
          foundDatastreamTask = getOrCreateDatastreamTask(reuseTasksPerDg, dg);
        }

        currentAssignmentCopy.get(instance).remove(foundDatastreamTask);
        newAssignment.get(instance).add(foundDatastreamTask);

        // Move to the next instance
        instancePos = (instancePos + 1) % instances.size();
      }
    }

    LOG.info("New assignment is {}", newAssignment);

    return newAssignment;
  }

  private DatastreamTask getOrCreateDatastreamTask(List<DatastreamTask> reuseTasksPerDg, DatastreamGroup dg) {
    if (reuseTasksPerDg.size() > 0) {
      return reuseTasksPerDg.remove(0);
    }
    return new DatastreamTaskImpl(dg.getDatastreams());
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
