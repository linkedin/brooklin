/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;


/**
 * This Assignment Strategy follows the following rules:
 * <ol type="a">
 *   <li>
 *    The number of tasks for each datastream can be different. There will be a default value in case the datastream
 *    does not have one defined.</li>
 *   <li>
 *    For each datastream the number of tasks might be greater than the number of instances. The default is 1.</li>
 *   <li>
 *    The differences in the number of tasks assigned between any two instances should be less than or equal to
 *    the imbalance threshold, otherwise, it will trigger a rebalance.</li>
 *   <li>
 *    Try to preserve previous tasks assignment, in order to minimize the number of task movements.</li>
 *   <li>
 *    The tasks for a datastream might not be balanced across the instances; however, the tasks across all datastreams
 *    will be balanced such that condition (c) above is held.</li>
 * </ol>
 *
 * How this strategy works?
 * It does the assignment in three steps:
 * <ul>
 *  <li>
 *  Step 1: Copy tasks assignments from previous instances, if possible.</li><li>
 *  Step 2: Distribute the unallocated tasks, trying to fill up the instances with the lowest number of tasks.</li><li>
 *  Step 3: Move tasks from the instances with high tasks counts, to instances with low task count, in order
 *          to re-balance the cluster.</li>
 * </ul>
 */
public class StickyMulticastStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(StickyMulticastStrategy.class.getName());
  private static final Integer DEFAULT_IMBALANCE_THRESHOLD = 1;

  private final Optional<Integer> _maxTasks;
  private final Integer _imbalanceThreshold;

  /**
   * Constructor for StickyMulticastStrategy
   * @param maxTasks Maximum number of {@link DatastreamTask}s to create out
   *                 of any {@link com.linkedin.datastream.common.Datastream}
   *                 if no value is specified for the "maxTasks" config property
   *                 at an individual datastream level.
   * @param imbalanceThreshold The maximum allowable difference in the number of tasks assigned
   *                           between any two {@link com.linkedin.datastream.server.Coordinator}
   *                           instances, before triggering a rebalance. The default is
   *                           {@value DEFAULT_IMBALANCE_THRESHOLD}.
   */
  public StickyMulticastStrategy(Optional<Integer> maxTasks, Optional<Integer> imbalanceThreshold) {
    _maxTasks = maxTasks;
    _imbalanceThreshold = imbalanceThreshold.orElse(DEFAULT_IMBALANCE_THRESHOLD);

    if (_imbalanceThreshold < 1) {
      throw new IllegalArgumentException("Imbalance threshold must be larger or equal than 1");
    }
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    int totalAssignedTasks = currentAssignment.values().stream().mapToInt(Set::size).sum();
    LOG.info("Begin assign {} datastreams to {} instances with {} tasks", datastreams.size(), instances.size(),
        totalAssignedTasks);

    if (instances.isEmpty()) {
      // Nothing to do.
      return Collections.emptyMap();
    }

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    Map<String, Set<DatastreamTask>> currentAssignmentCopy = new HashMap<>(currentAssignment.size());
    currentAssignment.forEach((k, v) -> currentAssignmentCopy.put(k, new HashSet<>(v)));

    for (String instance : instances) {
      newAssignment.put(instance, new HashSet<>());
    }

    // Data structure to keep track of the pending tasks to create
    Map<DatastreamGroup, Integer> unallocated = new HashMap<>();

    // STEP1: keep assignments from previous instances, if possible.
    for (DatastreamGroup dg : datastreams) {
      int numTasks = getNumTasks(dg, instances.size());
      for (String instance : instances) {
        if (numTasks <= 0) {
          break; // exit loop;
        }
        List<DatastreamTask> foundDatastreamTasks = Optional.ofNullable(currentAssignmentCopy.get(instance))
            .map(c -> c.stream().filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix())).collect(Collectors.toList()))
            .orElse(Collections.emptyList());

        if (!foundDatastreamTasks.isEmpty()) {
          newAssignment.get(instance).addAll(foundDatastreamTasks);
          currentAssignmentCopy.get(instance).removeAll(foundDatastreamTasks);
          numTasks -= foundDatastreamTasks.size();
        }
      }
      if (numTasks > 0) {
        unallocated.put(dg, numTasks);
      }
    }

    // Create a helper structure to keep all instances sorted by size.
    List<String> instancesBySize = new ArrayList<>(instances);

    //Shuffle the instances so that we may get a slightly different assignment for each unassigned task
    Collections.shuffle(instancesBySize);

    instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));

    // STEP2: Distribute the unallocated tasks to the instances with the lowest number of tasks.
    for (DatastreamGroup dg : unallocated.keySet()) {
      int pendingTasks = unallocated.get(dg);

      while (pendingTasks > 0) {
        // round-robin to the instances
        for (String instance : instancesBySize) {
          DatastreamTask task = new DatastreamTaskImpl(dg.getDatastreams());
          newAssignment.get(instance).add(task);
          pendingTasks--;
          if (pendingTasks == 0) {
            break;
          }
        }

        // sort the instance to preserve the invariance
        instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));
      }
    }

    //STEP 3: Trigger rebalance if the number of different tasks more than the configured threshold
    if (newAssignment.get(instancesBySize.get(instancesBySize.size() - 1)).size()
        - newAssignment.get(instancesBySize.get(0)).size() > _imbalanceThreshold) {
      int tasksTotal = newAssignment.values().stream().mapToInt(Set::size).sum();
      int minTasksPerInstance = tasksTotal / instances.size();

      // some rebalance to increase the task count in instances below the minTasksPerInstance
      while (newAssignment.get(instancesBySize.get(0)).size() < minTasksPerInstance) {
        String smallInstance = instancesBySize.get(0);
        String largeInstance = instancesBySize.get(instancesBySize.size() - 1);

        // Look for tasks that can be move from the large instance to the small instance
        for (DatastreamTask task : new ArrayList<>(newAssignment.get(largeInstance))) {
          newAssignment.get(largeInstance).remove(task);
          newAssignment.get(smallInstance).add(task);
          if (newAssignment.get(smallInstance).size() >= minTasksPerInstance
              || newAssignment.get(largeInstance).size() <= minTasksPerInstance + 1) {
            break;
          }
        }

        // sort the instance to preserve the invariance
        instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));
      }
    }

    // STEP4: Format the result with the right data structure.
    LOG.info("Assignment completed");
    LOG.debug("New assignment is {}", newAssignment);

    // STEP5: Some Sanity Checks, to detect missing tasks.
    sanityChecks(datastreams, instances, newAssignment);

    return newAssignment;
  }

  private void sanityChecks(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> result) {
    Map<String, Long> taskCountByTaskPrefix = result.values()
        .stream()
        .flatMap(Collection::stream)
        .map(DatastreamTask::getTaskPrefix)
        .collect(Collectors.groupingBy(prefix -> prefix, Collectors.counting()));

    List<String> validations = new ArrayList<>();
    for (DatastreamGroup dg : datastreams) {
      long numTasks = getNumTasks(dg, instances.size());
      long actualNumTasks = taskCountByTaskPrefix.get(dg.getTaskPrefix());
      if (numTasks != actualNumTasks) {
        validations.add(String.format("Missing tasks for %s. Actual: %d, expected: %d", dg, actualNumTasks, numTasks));
      }
    }

    if (!validations.isEmpty()) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment: %s", validations));
    }
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
