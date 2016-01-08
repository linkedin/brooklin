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
import java.util.Set;


public class SimpleStrategy implements AssignmentStrategy {

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<Datastream> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {
    // if there are no live instances, return empty assignment
    if (instances.size() == 0) {
      return new HashMap<>();
    }

    Collections.sort(instances);
    datastreams = sortDatastreams(datastreams);

    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();

    for (int i = 0; i < datastreams.size(); i++) {
      int instanceIndex = i % instances.size();
      assign(instances.get(instanceIndex), datastreams.get(i), assignment, currentAssignment);
    }

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

  private void assign(String instance, Datastream datastream, Map<String, Set<DatastreamTask>> assignment,
                      Map<String, Set<DatastreamTask>> currentAssignment) {
    if (!assignment.containsKey(instance)) {
      assignment.put(instance, new HashSet<>());
    }

    if (currentAssignment != null) {
      // Look for the task sharing the same destination across all instances
      for (String currentInstance : currentAssignment.keySet()) {
        for (DatastreamTask task : currentAssignment.get(currentInstance)) {
          DatastreamTaskImpl taskImpl = (DatastreamTaskImpl) task;
          // TODO: need to account for partitions which cannot be done with destination
          if (datastream.getDestination().equals(taskImpl.getDatastream().getDestination())) {
            assignment.get(instance).add(taskImpl);
            return;
          }
        }
      }
    }

    // No existing task found for the destination so create a new task
    assignment.get(instance).add(new DatastreamTaskImpl(datastream));
  }
}
