package com.linkedin.datastream.server.assignment;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.DatastreamTaskStatus;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Creates the datastreamTasks only for the new datastreams. The number of tasks created for datastream is
 * min(NumberOfPartitionsInDatastream, numberOfInstances * OVER_PARTITIONING_FACTOR).
 * These datastreamTasks are sorted by the datastreamName.
 * These tasks are then redistributed across all the instances equally.
 *
 * The strategy skips all tasks whose status is
 * {@link com.linkedin.datastream.server.DatastreamTaskStatus.Code#COMPLETE}.
 */
public class LoadbalancingStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(LoadbalancingStrategy.class.getName());

  public static final int DEFAULT_OVER_PARTITIONING_FACTOR = 2;
  public static final String CFG_OVER_PARTITIONING_FACTOR = "overPartitioningFactor";

  public static final String CFG_MIN_TASKS = "TasksPerDatastream";
  public static final int DEFAULT_MIN_TASKS = -1;


  // If the instances are down while the assignment is happening. We need to ensure that at least min tasks are created
  private final int minTasks;
  private final int overPartitioningFactor;

  public LoadbalancingStrategy() {
    this(new Properties());
  }

  public LoadbalancingStrategy(Properties properties) {
    VerifiableProperties props = new VerifiableProperties(properties);
    overPartitioningFactor = props.getInt(CFG_OVER_PARTITIONING_FACTOR, DEFAULT_OVER_PARTITIONING_FACTOR);
    minTasks = props.getInt(CFG_MIN_TASKS, DEFAULT_MIN_TASKS);
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<Datastream> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    // if there are no live instances, return empty assignment
    if (instances.size() == 0) {
      return new HashMap<>();
    }

    int maxTasksPerDatastream = Math.max(instances.size() * overPartitioningFactor, minTasks);
    datastreams = sortDatastreams(datastreams);
    List<DatastreamTask> datastreamTasks = getDatastreamTasks(datastreams, currentAssignment, maxTasksPerDatastream);
    Collections.sort(instances);

    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();
    instances.forEach(i -> assignment.put(i, new HashSet<>()));

    // Distribute the datastream tasks across all the instances.
    int datastreamTaskLength = datastreamTasks.size();
    for (int i = 0; i < datastreamTaskLength; i++) {
      String instanceName = instances.get(i % instances.size());
      assignment.get(instanceName).add(datastreamTasks.get(i));
    }

    LOG.info(String.format("Datastreams: %s, instances: %s, currentAssignment: %s, NewAssignment: %s",
        datastreams.stream().map(x -> x.getName()).collect(Collectors.toList()), instances, currentAssignment,
        assignment));

    return assignment;
  }

  private List<Datastream> sortDatastreams(List<Datastream> datastreams) {
    Map<String, Datastream> m = new HashMap<>();
    List<String> keys = new ArrayList<>();
    datastreams.forEach(ds -> {
      m.put(ds.getName(), ds);
      keys.add(ds.getName());
    });

    Collections.sort(keys);

    List<Datastream> result = new ArrayList<>();
    keys.forEach(k -> result.add(m.get(k)));

    return result;
  }

  private List<DatastreamTask> getDatastreamTasks(List<Datastream> datastreams,
      Map<String, Set<DatastreamTask>> currentAssignment, int maxTasksPerDatastream) {
    Set<DatastreamTask> allTasks = new HashSet<>();

    List<DatastreamTask> currentlyAssignedDatastreamTasks = new ArrayList<>();
    currentAssignment.values().forEach(currentlyAssignedDatastreamTasks::addAll);

    for (Datastream datastream : datastreams) {
      Set<DatastreamTask> tasksForDatastream = currentlyAssignedDatastreamTasks.stream()
          .filter(
              x -> x.getDatastreams().stream().map(Datastream::getName).anyMatch(y -> y.contains(datastream.getName())))
          .collect(Collectors.toSet());

      // If there are no datastream tasks that are currently assigned for this datastream.
      if (tasksForDatastream.isEmpty()) {
        tasksForDatastream = createTasksForDatastream(datastream, maxTasksPerDatastream);
      } else {
        // Filter out any COMPLETE tasks
        tasksForDatastream = tasksForDatastream.stream()
            .filter(t -> t.getStatus() == null || t.getStatus().getCode() != DatastreamTaskStatus.Code.COMPLETE)
            .collect(Collectors.toSet());
      }

      allTasks.addAll(tasksForDatastream);
    }

    return new ArrayList<>(allTasks);
  }

  private Set<DatastreamTask> createTasksForDatastream(Datastream datastream, int maxTasksPerDatastream) {
    int numberOfDatastreamPartitions = 1;
    if (datastream.hasSource() && datastream.getSource().hasPartitions()) {
      numberOfDatastreamPartitions = datastream.getSource().getPartitions();
    }
    int tasksPerDatastream =
        maxTasksPerDatastream < numberOfDatastreamPartitions ? maxTasksPerDatastream : numberOfDatastreamPartitions;
    Set<DatastreamTask> tasks = new HashSet<>();
    for (int index = 0; index < tasksPerDatastream; index++) {
      tasks.add(new DatastreamTaskImpl(datastream, Integer.toString(index),
          assignPartitionsToTask(datastream, index, tasksPerDatastream)));
    }

    return tasks;
  }

  // Finds the list of partitions that the current datastream task should own.
  // Based on the task index, total number of tasks in the datastream and the total partitions in the datastream source
  private List<Integer> assignPartitionsToTask(Datastream datastream, int taskIndex, int totalTasks) {
    List<Integer> partitions = new ArrayList<>();
    if (datastream.hasSource() && datastream.getSource().hasPartitions()) {
      int numPartitions = datastream.getSource().getPartitions();
      for (int index = 0; index + taskIndex < numPartitions; index += totalTasks) {
        partitions.add(index + taskIndex);
      }
    }

    return partitions;
  }
}
