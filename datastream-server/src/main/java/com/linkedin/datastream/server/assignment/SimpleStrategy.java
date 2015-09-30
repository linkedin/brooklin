package com.linkedin.datastream.server.assignment;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.AssignmentStrategy;
import com.linkedin.datastream.server.DatastreamTask;

import java.util.*;


public class SimpleStrategy implements AssignmentStrategy {

  @Override
  public Map<String, List<DatastreamTask>> assign(List<Datastream> datastreams, List<String> instances,
      Map<String, List<DatastreamTask>> currentAssignment) {
    // if there are no live instances, return empty assignment
    if (instances.size() == 0) {
      return new HashMap<>();
    }

    Collections.sort(instances);
    datastreams = sortDatastreams(datastreams);

    Map<String, List<DatastreamTask>> assignment = new HashMap<>();

    for (int i = 0; i < datastreams.size(); i++) {
      int instanceIndex = i % instances.size();
      assign(instances.get(instanceIndex), datastreams.get(i), assignment);
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

  private void assign(String instance, Datastream datastream, Map<String, List<DatastreamTask>> assignment) {
    DatastreamTask datastreamTask = new DatastreamTask(datastream);
    if (!assignment.containsKey(instance)) {
      assignment.put(instance, new ArrayList<>());
    }
    assignment.get(instance).add(datastreamTask);
  }
}
