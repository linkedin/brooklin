package com.linkedin.datastream.server.assignment;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;


/**
 * Creates the datastreamTasks only for the new datastreams. The number of tasks created for datastream is
 * min(NumberOfPartitionsInDatastream, numberOfInstances * OVER_PARTITIONING_FACTOR).
 * These datastreamTasks are sorted by the datastreamName.
 * These tasks are then redistributed across all the instances equally.
 */
public class LoadbalancingStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(LoadbalancingStrategy.class.getName());

  public static final int DEFAULT_OVER_PARTITIONING_FACTOR = 2;
  public static final String CFG_OVER_PARTITIONING_FACTOR = "_overPartitioningFactor";
  public static final String CFG_USE_MAX_TASKS_FOR_PARTITIONING = "useMaxTaskForPartitioning";
  public static final String CFG_MAX_TASKS = "maxTasks";
  public static final String CFG_MIN_TASKS = "TasksPerDatastream";
  public static final String CFG_IGNORE_PARTITION_CORRUPTIONS = "ignorePartitionCorruptions";

  public static final int DEFAULT_MIN_TASKS = -1;
  public static final boolean DEFAULT_USE_MAX_TASKS_FOR_PARTITIONING = false;
  public static final int DEFAULT_MAX_TASKS = 10;
  public static final boolean DEFAULT_IGNORE_PARTITION_CORRUPTIONS = true;

  // If the instances are down while the assignment is happening. We need to ensure that at least min tasks are created
  private final int _minTasks;
  private final int _overPartitioningFactor;
  private final int _maxTasks;
  private final boolean _useMaxTaskForPartitioning;
  private final boolean _ignorePartitionCorruptions;

  public LoadbalancingStrategy() {
    this(new Properties());
  }

  public LoadbalancingStrategy(Properties properties) {
    VerifiableProperties props = new VerifiableProperties(properties);

    _overPartitioningFactor = props.getInt(CFG_OVER_PARTITIONING_FACTOR, DEFAULT_OVER_PARTITIONING_FACTOR);
    _minTasks = props.getInt(CFG_MIN_TASKS, DEFAULT_MIN_TASKS);
    _maxTasks = props.getInt(CFG_MAX_TASKS, DEFAULT_MAX_TASKS);
    _useMaxTaskForPartitioning = props.getBoolean(CFG_USE_MAX_TASKS_FOR_PARTITIONING, DEFAULT_USE_MAX_TASKS_FOR_PARTITIONING);
    _ignorePartitionCorruptions = props.getBoolean(CFG_IGNORE_PARTITION_CORRUPTIONS, DEFAULT_IGNORE_PARTITION_CORRUPTIONS);
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {
    LOG.info(String.format("Assign called with datastreams: %s, instances: %s, currentAssignment: %s", datastreams,
        instances, currentAssignment));
    // if there are no live instances, return empty assignment
    if (instances.size() == 0) {
      return new HashMap<>();
    }

    int maxTasksPerDatastream = Math.max(instances.size() * _overPartitioningFactor, _minTasks);
    List<DatastreamTask> datastreamTasks = getDatastreamTasks(datastreams, currentAssignment, maxTasksPerDatastream);
    datastreamTasks.sort(Comparator.comparing(DatastreamTask::getDatastreamTaskName));
    instances.sort(String::compareTo);

    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();
    instances.forEach(i -> assignment.put(i, new HashSet<>()));

    // Distribute the datastream tasks across all the instances.
    int datastreamTaskLength = datastreamTasks.size();
    for (int i = 0; i < datastreamTaskLength; i++) {
      String instanceName = instances.get(i % instances.size());
      assignment.get(instanceName).add(datastreamTasks.get(i));
    }

    LOG.info(String.format("Datastream Groups: %s, instances: %s, currentAssignment: %s, NewAssignment: %s",
        datastreams.stream().map(DatastreamGroup::getTaskPrefix).collect(Collectors.toList()), instances,
        currentAssignment, assignment));

    return assignment;
  }

  private List<DatastreamTask> getDatastreamTasks(List<DatastreamGroup> datastreams,
      Map<String, Set<DatastreamTask>> currentAssignment, int maxTasksPerDatastream) {
    Set<DatastreamTask> allTasks = new HashSet<>();

    List<DatastreamTask> currentlyAssignedDatastreamTasks = new ArrayList<>();
    currentAssignment.values().forEach(currentlyAssignedDatastreamTasks::addAll);

    boolean isSuccess = true;
    for (DatastreamGroup dg : datastreams) {
      Set<DatastreamTask> tasksForDatastreamGroup = currentlyAssignedDatastreamTasks.stream()
          .filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()))
          .collect(Collectors.toSet());

      // If there are no datastream tasks that are currently assigned for this datastream.
      if (tasksForDatastreamGroup.isEmpty()) {
        tasksForDatastreamGroup = createTasksForDatastream(dg, maxTasksPerDatastream);
      } else {
        Datastream datastream = dg.getDatastreams().get(0);
        // Check that all partitions are covered with existing tasks.
        int numberOfDatastreamPartitions = getNumberOfDatastreamPartitions(dg);
        Set<Integer> assignedPartitions = new HashSet<>();
        int count = 0;
        for (DatastreamTask task : tasksForDatastreamGroup) {
          count += task.getPartitions().size();
          assignedPartitions.addAll(task.getPartitions());
        }
        if (count != numberOfDatastreamPartitions || assignedPartitions.size() != numberOfDatastreamPartitions) {
          LOG.warn(
              "Corrupted partition information for datastream {}. Expected number of partitions {}, actual {}: {}",
              datastream.getName(), numberOfDatastreamPartitions, count, assignedPartitions);
          isSuccess = false;
        }
      }

      if (isSuccess || _ignorePartitionCorruptions) {
        allTasks.addAll(tasksForDatastreamGroup);
      } else {
        LOG.error("Ignoring tasks for datastream {} since partition information is corrupted. Fix info for rebalance "
            + "to reassign tasks", dg.getDatastreams().get(0).getName());
      }
    }

    return new ArrayList<>(allTasks);
  }

  private int getNumberOfDatastreamPartitions(DatastreamGroup datastream) {
    return (_useMaxTaskForPartitioning ? _maxTasks :
        datastream.getSourcePartitions().orElse(1));
  }

  private Set<DatastreamTask> createTasksForDatastream(DatastreamGroup datastream, int maxTasksPerDatastream) {
    int numberOfDatastreamPartitions = getNumberOfDatastreamPartitions(datastream);
    int tasksPerDatastream =
        maxTasksPerDatastream < numberOfDatastreamPartitions ? maxTasksPerDatastream : numberOfDatastreamPartitions;
    Set<DatastreamTask> tasks = new HashSet<>();
    for (int index = 0; index < tasksPerDatastream; index++) {
      tasks.add(new DatastreamTaskImpl(datastream.getDatastreams(), Integer.toString(index),
          assignPartitionsToTask(numberOfDatastreamPartitions, index, tasksPerDatastream), tasksPerDatastream));
    }

    return tasks;
  }

  // Finds the list of partitions that the current datastream task should own.
  // Based on the task index, total number of tasks in the datastream and the total partitions in the datastream source
  private List<Integer> assignPartitionsToTask(int numPartitions, int taskIndex, int totalTasks) {
    List<Integer> partitions = new ArrayList<>();
    for (int index = 0; index + taskIndex < numPartitions; index += totalTasks) {
      partitions.add(index + taskIndex);
    }

    return partitions;
  }
}
