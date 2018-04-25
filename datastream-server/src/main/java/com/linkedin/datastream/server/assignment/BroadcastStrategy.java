package com.linkedin.datastream.server.assignment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;

/**
 * The number of tasks created for datastream is configurable using "maxTasks" config. This can also be overriden at the
 * Datastream level via the Datastream metadata "maxTasks". The number of tasks is not necessarily capped at the
 * number of instances, so each instance could process multiple tasks for the same Datastream. The maximum number of
 * datastream tasks (for the same datastream) that an instance will be assigned is configurable.
 *
 * All the tasks are redistributed across all the instances equally.
 */
public class BroadcastStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(BroadcastStrategy.class.getName());

  private final int _maxTasks;
  private final int _dsTaskLimitPerInstance;

  public BroadcastStrategy(int maxTasks) {
    this(maxTasks, 1);
  }

  public BroadcastStrategy(int maxTasks, int dsTaskLimitPerInstance) {
    Validate.inclusiveBetween(1, Integer.MAX_VALUE, maxTasks,
        "Default maxTasks should be between 1 and Integer.MAX_VALUE");
    Validate.inclusiveBetween(1, 10000, dsTaskLimitPerInstance,
        "Default dsTaskLimitPerInstance should be between 1 and 10000");
    _maxTasks = maxTasks;
    _dsTaskLimitPerInstance = dsTaskLimitPerInstance;
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    LOG.info("Trying to assign datastreams {} to instances {} and the current assignment is {}", datastreams, instances,
        currentAssignment);

    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    // Make a copy of the current assignment, since the strategy modifies it during calculation
    Map<String, Set<DatastreamTask>> currentAssignmentCopy = new HashMap<>(currentAssignment.size());
    currentAssignment.forEach((k, v) -> currentAssignmentCopy.put(k, new HashSet<>(v)));

    for (String instance : instances) {
      newAssignment.put(instance, new HashSet<>());
      currentAssignmentCopy.putIfAbsent(instance, new HashSet<>());
    }

    int instancePos = 0;
    for (DatastreamGroup dg : datastreams) {
      int numTasks = getNumTasks(dg, instances.size());
      for (int taskPos = 0; taskPos < numTasks; taskPos++) {
        String instance = instances.get(instancePos);

        DatastreamTask foundDatastreamTask = currentAssignmentCopy.get(instance)
            .stream()
            .filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()))
            .findFirst()
            .orElse(new DatastreamTaskImpl(dg.getDatastreams()));

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
    int numTasks = dg.getDatastreams().stream()
        .map(ds -> ds.getMetadata().get(CFG_MAX_TASKS))
        .filter(Objects::nonNull)
        .mapToInt(Integer::valueOf)
        .map(x -> x < 1 ? Integer.MAX_VALUE : x)
        .max()
        .orElse(_maxTasks);
    return Math.min(numTasks, numInstances * _dsTaskLimitPerInstance);
  }

}
