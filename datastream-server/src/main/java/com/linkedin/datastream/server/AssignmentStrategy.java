package com.linkedin.datastream.server;

import java.util.List;
import java.util.Map;

import com.linkedin.datastream.common.Datastream;


public interface AssignmentStrategy {
  /**
   * assign a list of datastreams to a list of instances, and return a map from instances-> list of streams.
   * Each connector is associated with one implementation of the AssignmentStrategy, and it is only called
   * by the Coordinator Leader.
   *
   * <p>The <i>assign</i> method takes as input the current list of live instances, current list of
   * Datastreams, and optionally the current assignment, returns a new assignment.
   *
   * <p>Note that the output is a map from instance to list of DatastreamTask instead of Datastream.
   * {@link com.linkedin.datastream.server.DatastreamTask} is the minimum assignable element of Datastream.
   * This makes it possible to split a Datastream into multiple assignable DatastreamTask so that they
   * can be assigned to multiple instances, and hence allowing load balancing. For example, the Oracle
   * bootstrap Datastream can be splitted into multiple instances of DatastreamTask, one per partition,
   * if the bootstrap files are hosted on HDFS. This allows the concurrent processing of the partitions
   * to maximize the network IO.
   *
   * @param datastreams all data streams defined in Datastream Management Service to be assigned
   * @param instances all live instances
   * @param currentAssignment existing assignment
   * @return
   */
  Map<String, List<DatastreamTask>> assign(List<Datastream> datastreams, List<String> instances,
      Map<String, List<DatastreamTask>> currentAssignment);
}
