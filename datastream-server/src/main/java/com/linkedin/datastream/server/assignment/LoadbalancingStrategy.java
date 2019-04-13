/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
 * These tasks are then evenly redistributed across all the instances.
 */
public class LoadbalancingStrategy implements AssignmentStrategy {

  public static final int DEFAULT_OVER_PARTITIONING_FACTOR = 2;
  public static final String CFG_OVER_PARTITIONING_FACTOR = "overPartitioningFactor";
  public static final String CFG_MIN_TASKS = "TasksPerDatastream";
  public static final int DEFAULT_MIN_TASKS = -1;

  private static final Logger LOG = LoggerFactory.getLogger(LoadbalancingStrategy.class.getName());

  // If the instances are down while the assignment is happening. We need to ensure that at least min tasks are created
  private final int minTasks;
  private final int overPartitioningFactor;

  /**
   * Construct an instance of LoadbalancingStrategy
   */
  public LoadbalancingStrategy() {
    this(new Properties());
  }

  /**
   * Construct an instance of LoadbalancingStrategy
   * @param properties Configuration properties to load
   */
  public LoadbalancingStrategy(Properties properties) {
    VerifiableProperties props = new VerifiableProperties(properties);
    overPartitioningFactor = props.getInt(CFG_OVER_PARTITIONING_FACTOR, DEFAULT_OVER_PARTITIONING_FACTOR);
    minTasks = props.getInt(CFG_MIN_TASKS, DEFAULT_MIN_TASKS);
  }

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {
    LOG.info("Assign called with datastreams: {}, instances: {}, currentAssignment: {}", datastreams,
        instances, currentAssignment);
    // if there are no live instances, return empty assignment
    if (instances.size() == 0) {
      return new HashMap<>();
    }

    int maxTasksPerDatastream = Math.max(instances.size() * overPartitioningFactor, minTasks);
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

    LOG.info("Datastream Groups: {}, instances: {}, currentAssignment: {}, NewAssignment: {}",
        datastreams.stream().map(DatastreamGroup::getTaskPrefix).collect(Collectors.toList()), instances,
        currentAssignment, assignment);

    return assignment;
  }

  private List<DatastreamTask> getDatastreamTasks(List<DatastreamGroup> datastreams,
      Map<String, Set<DatastreamTask>> currentAssignment, int maxTasksPerDatastream) {
    Set<DatastreamTask> allTasks = new HashSet<>();

    List<DatastreamTask> currentlyAssignedDatastreamTasks = new ArrayList<>();
    currentAssignment.values().forEach(currentlyAssignedDatastreamTasks::addAll);

    for (DatastreamGroup dg : datastreams) {
      Set<DatastreamTask> tasksForDatastreamGroup = currentlyAssignedDatastreamTasks.stream()
          .filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()))
          .collect(Collectors.toSet());

      // If there are no datastream tasks that are currently assigned for this datastream.
      if (tasksForDatastreamGroup.isEmpty()) {
        tasksForDatastreamGroup = createTasksForDatastream(dg, maxTasksPerDatastream);
      } else {
        Datastream datastream = dg.getDatastreams().get(0);
        if (datastream.hasSource() && datastream.getSource().hasPartitions()) {
          // Check that all partitions are covered with existing tasks.
          int numberOfDatastreamPartitions = datastream.getSource().getPartitions();
          Set<Integer> assignedPartitions = new HashSet<>();
          int count = 0;
          for (DatastreamTask task : tasksForDatastreamGroup) {
            count += task.getPartitions().size();
            assignedPartitions.addAll(task.getPartitions());
          }
          if (count != numberOfDatastreamPartitions || assignedPartitions.size() != numberOfDatastreamPartitions) {
            LOG.error(
                "Corrupted partition information for datastream {}. Expected number of partitions {}, actual {}: {}",
                datastream.getName(), numberOfDatastreamPartitions, count, assignedPartitions);
          }
        }
      }

      allTasks.addAll(tasksForDatastreamGroup);
    }

    return new ArrayList<>(allTasks);
  }

  private Set<DatastreamTask> createTasksForDatastream(DatastreamGroup datastream, int maxTasksPerDatastream) {
    int numberOfDatastreamPartitions = datastream.getSourcePartitions().orElse(1);
    int tasksPerDatastream =
        maxTasksPerDatastream < numberOfDatastreamPartitions ? maxTasksPerDatastream : numberOfDatastreamPartitions;
    Set<DatastreamTask> tasks = new HashSet<>();
    for (int index = 0; index < tasksPerDatastream; index++) {
      tasks.add(new DatastreamTaskImpl(datastream.getDatastreams(), Integer.toString(index),
          assignPartitionsToTask(numberOfDatastreamPartitions, index, tasksPerDatastream)));
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
