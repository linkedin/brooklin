package com.linkedin.datastream.server.assignment;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.server.AssignmentStrategy;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class BroadcastStrategy implements AssignmentStrategy {
  @Override
  public Map<String, List<DatastreamTask>> assign(List<Datastream> datastreams, List<String> instances,
      Map<String, List<DatastreamTask>> currentAssignment) {
    Map<String, List<DatastreamTask>> assignment = new HashMap<>();

    List<DatastreamTask> templateAssignment = new ArrayList<>();
    for (Datastream stream : datastreams) {
      templateAssignment.add(new DatastreamTaskImpl(stream));
    }

    for (String instance : instances) {
      // make a clone from the template so they are not sharing the same references
      List<DatastreamTask> clone = new ArrayList<>(templateAssignment);
      assignment.put(instance, clone);
    }

    return assignment;
  }
}
