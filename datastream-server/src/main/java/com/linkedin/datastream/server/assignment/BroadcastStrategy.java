package com.linkedin.datastream.server.assignment;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;


public class BroadcastStrategy implements AssignmentStrategy {

  private static final Logger LOG = LoggerFactory.getLogger(BroadcastStrategy.class.getName());

  @Override
  public Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment) {

    LOG.info(String.format("Trying to assign datastreams {%s} to instances {%s} and the current assignment is {%s}",
        datastreams, instances, currentAssignment));

    Map<String, Set<DatastreamTask>> assignment = new HashMap<>();

    for (String instance : instances) {
      Set<DatastreamTask> newAssignmentForInstance = assignment.computeIfAbsent(instance, (x) -> new HashSet<>());
      Set<DatastreamTask> currentAssignmentForInstance =
          currentAssignment.computeIfAbsent(instance, (x) -> new HashSet<>());

      for (DatastreamGroup dg : datastreams) {
        DatastreamTask foundDatastreamTask = currentAssignmentForInstance.stream()
            .filter(x -> x.getTaskPrefix().equals(dg.getTaskPrefix()))
            .findFirst()
            .orElse(new DatastreamTaskImpl(dg.getDatastreams()));
        newAssignmentForInstance.add(foundDatastreamTask);
      }
    }

    LOG.info(String.format("New assignment is {%s}", assignment));

    return assignment;
  }
}
