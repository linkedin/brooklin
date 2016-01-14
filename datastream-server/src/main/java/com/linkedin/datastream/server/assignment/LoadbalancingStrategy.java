package com.linkedin.datastream.server.assignment;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.AssignmentStrategy;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.function.BinaryOperator;
import java.util.stream.Collectors;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.Math.ceil;
import static java.lang.Math.floor;


/**
 * Some of the design goals of the LoadBalancing Strategy in the order of priority
 * 1. Load balance the tasks equally across all the instances
 * 2. Ensure that there is minimum movement of tasks across the instances in the following scenarios
 *   (Datastream Add, Datastream Delete, Instance Add, Instance Remove).
 * 3. Distribute the tasks for the same datastream across different instances.
 *
 * The way the algorithm achieves the above priorities is by finding the minTasksPerInstance and maxTasksPerInstance
 * minTasksPerInstance will be equals to maxTasksPerInstance if the number of tasks is exactly divisible by instances.
 *
 * Algorithm has two phases
 * Phase 1: Algorithm assigns the minTasksPerInstance to all the existing instances using the current Assignment. If the
 * number of instances in the currentASsignment for an instance < minTasksPerInstance It will assign only those existing instances.
 *
 * Phase 2: Algorithm distributes the remaining tasks one by one across all the instances, When an instance has
 * maxTasksPerInstance tasks, It is skipped.
 *
 */
public class LoadbalancingStrategy implements AssignmentStrategy {
  private static final Logger LOG = LoggerFactory.getLogger(LoadbalancingStrategy.class.getName());

  private static final int PARTITIONING_FACTOR = 2;

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<Datastream> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    int numberOfInstances = instances.size();
    int maxTasksPerDatastream = instances.size() * PARTITIONING_FACTOR;

    Set<DatastreamTask> datastreamTasks = getDatastreamTasks(datastreams, currentAssignment, maxTasksPerDatastream);
    // if there are no live instances, return empty assignment
    if (instances.size() == 0) {
      return new HashMap<>();
    }

    int minTasksPerInstance =  (int) floor(datastreamTasks.size() * 1.0 / instances.size());
    int maxTasksPerInstance = datastreamTasks.size() % instances.size() > 0 ? minTasksPerInstance + 1 : minTasksPerInstance;
    Map<String, Set<DatastreamTask>> newAssignment = new HashMap<>();

    Collections.sort(instances);
    // First assign the tasks to the instances that were already present in the current assignment.
    // This is to ensure that there is minimum movement of tasks.
    for(String instance : instances) {
      if(currentAssignment.containsKey(instance)) {
        int addedTasksCount = 0;
        Set<DatastreamTask> newAssignmentForInstance = new HashSet<>();
        for(DatastreamTask task : currentAssignment.get(instance)) {
          if(addedTasksCount < minTasksPerInstance) {
            addedTasksCount++;
            newAssignmentForInstance.add(task);
            datastreamTasks.remove(task);
          }
        }
        newAssignment.put(instance, newAssignmentForInstance);
      } else {
        newAssignment.put(instance, new HashSet<>());
      }
    }

    // Distribute the remaining tasks to all the instances
    int index = 0;
    for(DatastreamTask datastreamTask : datastreamTasks) {
      String instance = instances.get(index%numberOfInstances);
      if(newAssignment.get(instance).size() >= maxTasksPerInstance) {
        continue;
      } else {
        newAssignment.get(instance).add(datastreamTask);
        index++;
      }
    }

    LOG.info(String.format("Datastreams: %s, instances: %s, currentAssignment: %s, NewAssignment: %s",
        datastreams.stream().map(x -> x.getName()).collect(Collectors.toList()),
        instances, currentAssignment, newAssignment));

    return newAssignment;
  }

  private Set<DatastreamTask> getDatastreamTasks(List<Datastream> datastreams,
      Map<String, Set<DatastreamTask>> currentAssignment, int maxTasksPerDatastream) {
    Set<DatastreamTask> allTasks = new HashSet<>();
    currentAssignment.values().forEach(allTasks::addAll);

    // Find if there are any datastreams for which the tasks needs to be created.
    // If so create the datastream tasks for those datastreams.
    for(Datastream datastream : datastreams) {
      Set<DatastreamTask> tasksForDatastream = new HashSet<>();
      allTasks.stream().filter(x -> x.getDatastreams().contains(datastream.getName())).forEach(tasksForDatastream::add);
      if(tasksForDatastream.isEmpty()) {
        tasksForDatastream = createTasksForDatastream(datastream, maxTasksPerDatastream);
        allTasks.addAll(tasksForDatastream);
      }
    }

    return allTasks;
  }

  private Set<DatastreamTask> createTasksForDatastream(Datastream datastream, int maxTasksPerDatastream) {
    int numberOfDatastreamPartitions = datastream.getDestination().getPartitions();
    int tasksPerDatastream = maxTasksPerDatastream < numberOfDatastreamPartitions ? maxTasksPerDatastream : numberOfDatastreamPartitions;
    Set<DatastreamTask> tasks = new HashSet<>();
    for(int index = 0; index < tasksPerDatastream; index++) {
      tasks.add(new DatastreamTaskImpl(datastream));
    }

    return tasks;
  }
}
