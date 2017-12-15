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
 * This Assignment Strategy follow the following rules:
 * a) The number of tasks for each datastream can be different. There will be a default value
 *    in case the datastream does not have one defined.
 * b) In any case, for each datastream the number of tasks cannot be greater than the number of instances.
 * c) At most one task from each datastream, can be assigned to each instances.
 * d) The differences on the number of tasks assigned between any two instances,
 *    should be less than or equal to one.
 * e) Try to preserve previous tasks assignment, in order to minimize the number of tasks
 *    movements.
 *
 * How this strategy works?
 * It does the assignment in three steps:
 * Step 1: Copy tasks assignments from previous instances, if possible.
 * Step 2: Distribute the unallocated tasks, trying to fill up the instances with the lowest number of tasks.
 * Step 3: Move tasks from the instances with high tasks counts, to instances with low task count, in order
 *         to re-balance the cluster.
 */
public class StickyMulticastStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(StickyMulticastStrategy.class.getName());

  private final int _maxTasks;

  public StickyMulticastStrategy(int maxTasks) {
    // If maxTasks is less than one, then just create one task for each instance.
    _maxTasks = maxTasks < 1 ? Integer.MAX_VALUE : maxTasks;
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    LOG.info(String.format("Trying to assign datastreams {%s} to instances {%s} and the current assignment is {%s}",
        datastreams, instances, currentAssignment));

    if (instances.size() == 0) {
      // Nothing to do.
      return Collections.emptyMap();
    }


    Map<String, Map<String, DatastreamTask>> oldAssignment = new HashMap<>();
    Map<String, Map<String, DatastreamTask>> newAssignment = new HashMap<>();

    for (String instance : instances) {
      newAssignment.put(instance, new HashMap<>());
      oldAssignment.put(instance, new HashMap<>());
      if (currentAssignment.containsKey(instance)) {
        currentAssignment.get(instance).forEach(t -> oldAssignment.get(instance).put(t.getTaskPrefix(), t));
      }
    }

    // Data structure to keep track of the pending tasks to create
    Map<DatastreamGroup, Integer> unallocated = new HashMap<>();

    // STEP1: keep assignments from previous instances, if possible.
    for (DatastreamGroup dg : datastreams) {
      int numTask = getNumTasks(dg, instances.size());
      for (String instance : instances) {
        if (numTask <= 0) {
          break; // exit loop;
        }
        Optional<DatastreamTask> foundDatastreamTask =
            Optional.ofNullable(oldAssignment.get(instance).get(dg.getTaskPrefix()));
        if (foundDatastreamTask.isPresent()) {
          newAssignment.get(instance).put(foundDatastreamTask.get().getTaskPrefix(), foundDatastreamTask.get());
          numTask--;
        }
      }
      if (numTask > 0) {
        unallocated.put(dg, numTask);
      }
    }

    // Create a helper structure to keep all instances sorted by size.
    List<String> instancesBySize = new ArrayList<>(instances);
    instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));

    // STEP2: Distribute the unallocated tasks, to the instances with the lowest number of tasks.
    for (DatastreamGroup dg : unallocated.keySet()) {
      int pendingTasks = unallocated.get(dg);

      for (String instance : instancesBySize) {
        if (!newAssignment.get(instance).containsKey(dg.getTaskPrefix())) {
          DatastreamTask task = new DatastreamTaskImpl(dg.getDatastreams());
          newAssignment.get(instance).put(task.getTaskPrefix(), task);
          pendingTasks--;
          if (pendingTasks == 0) {
            break;
          }
        }
      }
      if (pendingTasks > 0) {
        // This should never happen.
        throw new RuntimeException("Invalid state. There are more tasks than instances.");
      }

      // sort the instance to preserve the invariance
      instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));
    }

    // Target number of tasks per instances.
    int tasksTotal = newAssignment.values().stream().mapToInt(Map::size).sum();
    int minTasksPerInstance = tasksTotal / instances.size();

    // STEP3: Do some rebalance to increase the task count in instances below the minTasksPerInstance.
    while (newAssignment.get(instancesBySize.get(0)).size() < minTasksPerInstance) {
      String smallInstance = instancesBySize.get(0);
      String largeInstance = instancesBySize.get(instancesBySize.size() - 1);

      // Look for tasks that can be move from the large instance to the small instance
      for (DatastreamTask task : new ArrayList<>(newAssignment.get(largeInstance).values())) {
        if (!newAssignment.get(smallInstance).containsKey(task.getTaskPrefix())) {
          newAssignment.get(largeInstance).remove(task.getTaskPrefix());
          newAssignment.get(smallInstance).put(task.getTaskPrefix(), new DatastreamTaskImpl(task.getDatastreams()));
          if (newAssignment.get(smallInstance).size() >= minTasksPerInstance
              || newAssignment.get(largeInstance).size() <= minTasksPerInstance + 1) {
            break;
          }
        }
      }

      // sort the instance to preserve the invariance
      instancesBySize.sort(Comparator.comparing(x -> newAssignment.get(x).size()));
    }

    // STEP4: Format the result with the right data structure.
    Map<String, Set<DatastreamTask>> result = newAssignment.entrySet()
        .stream()
        .collect(Collectors.toMap(Map.Entry::getKey, p -> new HashSet<>(p.getValue().values())));

    LOG.info(String.format("New assignment is {%s}", result));

    // STEP5: Some Sanity Checks, to detect missing tasks.
    sanityChecks(datastreams, instances, result);

    return result;
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
      long numTask = getNumTasks(dg, instances.size());
      long actualNumTask = taskCountByTaskPrefix.get(dg.getTaskPrefix());
      if (numTask != actualNumTask) {
        validations.add(String.format("Missing tasks for %s. Actual: %d, expected: %d", dg, actualNumTask, numTask));
      }
    }

    if (!validations.isEmpty()) {
      throw new DatastreamRuntimeException(String.format("Validation failed after assignment: %s", validations));
    }
  }

  private int getNumTasks(DatastreamGroup dg, int numInstances) {
    // Look for an override in any of the datastream.
    // In case of multiple overrides, select the largest.
    // If not override is present then use the default "_maxTasks"
    int maxTasks = dg.getDatastreams()
        .stream()
        .map(ds -> ds.getMetadata().get(CFG_MAX_TASKS))
        .filter(Objects::nonNull)
        .mapToInt(Integer::valueOf)
        .map(x -> x < 1 ? Integer.MAX_VALUE : x)
        .max()
        .orElse(_maxTasks);
    return Math.min(maxTasks, numInstances);
  }

}
