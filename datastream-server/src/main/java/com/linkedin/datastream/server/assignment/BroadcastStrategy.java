package com.linkedin.datastream.server.assignment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.CFG_MAX_TASKS;

/**
 * The number of tasks created for datastream is min(numberOfInstances, maxTasks),
 * unless maxTasks is less than 1, in that case it create one task for each instance.
 *
 * All the tasks are redistributed across all the instances equally.
 */
public class BroadcastStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(BroadcastStrategy.class.getName());

  private final int _maxTasks;

  public BroadcastStrategy(int maxTasks) {
    // If maxTasks is less than one, then just create one task for each instance.
    _maxTasks = maxTasks < 1 ? Integer.MAX_VALUE : maxTasks;
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    LOG.info(String.format("Trying to assign datastreams {%s} to instances {%s} and the current assignment is {%s}",
        datastreams, instances, currentAssignment));

    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();

    for (String instance : instances) {
      assignment.put(instance, new HashSet<>());
      currentAssignment.computeIfAbsent(instance, (x) -> new HashSet<>());
    }

    int instancePos = 0;
    for (DatastreamGroup dg : datastreams) {
      int numTask = getNumTasks(dg, instances.size());
      for (int taskPos = 0; taskPos < numTask; taskPos++) {
        String instance = instances.get(instancePos);

        DatastreamTask foundDatastreamTask = currentAssignment.get(instance)
            .stream()
            .filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()))
            .findFirst()
            .orElse(new DatastreamTaskImpl(dg.getDatastreams()));
        assignment.get(instance).add(foundDatastreamTask);

        // Move to the next instance
        instancePos = (instancePos + 1) % instances.size();
      }
    }

    LOG.info(String.format("New assignment is {%s}", assignment));

    return assignment;
  }

  private int getNumTasks(DatastreamGroup dg, int numInstances) {
    // Look for an override in any of the datastream.
    // In case of multiple overrides, select the largest.
    // If not override is present then use the default "_maxTasks"
    int maxTasks = dg.getDatastreams().stream()
        .map(ds -> ds.getMetadata().get(CFG_MAX_TASKS))
        .filter(Objects::nonNull)
        .mapToInt(Integer::valueOf)
        .map(x -> x < 1 ? Integer.MAX_VALUE : x)
        .max()
        .orElse(_maxTasks);
    return  Math.min(maxTasks, numInstances);
  }
}
