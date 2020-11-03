/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.strategy;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.NotImplementedException;

import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * Abstraction to allow defining different strategies for:
 * <ol>
 *   <li>
 *     breaking down a {@link com.linkedin.datastream.common.Datastream} into one or more {@link DatastreamTask}s
 *   </li>
 *   <li>
 *     distributing {@link DatastreamTask}s among the available {@link com.linkedin.datastream.server.Coordinator}
 *     instances
 *   </li>
 * </ol>
 */
public interface AssignmentStrategy {
  /**
   * Assign a list of datastreams to a list of instances, and return a map from instances-> list of streams.
   * Each connector is associated with one implementation of the AssignmentStrategy, and it is only called
   * by the Coordinator Leader.
   *
   * <p>The <i>assign</i> method takes as input the current list of live instances, current list of
   * Datastreams, and optionally the current assignment, returns a new assignment.
   *
   * <p>Note that the output is a map from instance to list of DatastreamTask instead of Datastream.
   * {@link DatastreamTask} is the minimum assignable element of Datastream.
   * This makes it possible to split a Datastream into multiple assignable DatastreamTasks so that they
   * can be assigned to multiple instances, and hence allowing load balancing. For example, the Oracle
   * bootstrap Datastream can be split into multiple instances of DatastreamTask, one per partition,
   * if the bootstrap files are hosted on HDFS. This allows the concurrent processing of the partitions
   * to maximize the network IO.
   *
   * @param datastreams all data streams defined in Datastream Management Service to be assigned
   * @param instances all live instances
   * @param currentAssignment existing assignment
   */
  Map<String, Set<DatastreamTask>> assign(List<DatastreamGroup> datastreams, List<String> instances,
      Map<String, Set<DatastreamTask>> currentAssignment);


  /**
   * Assign partition for a particular datastream group to all the tasks in current assignment
   * It returns a map from instance -> tasks map with partition info stored in the task
   * This interface needs to be implemented if the Brooklin Coordinator is going to perform the
   * partition assignment.
   *
   *
   * @param currentAssignment the old assignment for all the datastream groups across all instances
   * @param datastreamPartitions the subscribed partitions for the particular datastream group
   * @return new assignment mapping
   */
  default Map<String, Set<DatastreamTask>> assignPartitions(Map<String,
      Set<DatastreamTask>> currentAssignment, DatastreamGroupPartitionsMetadata datastreamPartitions) {
    return currentAssignment;
  }

  /**
   * Move a partition for a datastream group according to the targetAssignment. While It moves a single datastream
   * group, it can move multiple partitions to different hosts which are from multiple calls
   *
   * It returns a map from instance -> tasks map with partition info stored in the task.
   *
   * This interface needs to be implemented if the Brooklin Coordinator is going to perform the
   * partition movement.
   *
   * @param currentAssignment the old assignment, it is a mapping from instance name -> datastream task. The mapping
   *                          should cover all datastream groups across all instances
   * @param targetAssignment the target assignment retrieved from Zookeeper, it is a mapping
   *                         from instance name -> topicPartition
   * @param partitionsMetadata the subscribed partitions metadata received from connector
   * @return new assignment
   */
  default Map<String, Set<DatastreamTask>> movePartitions(Map<String, Set<DatastreamTask>> currentAssignment,
      Map<String, Set<String>> targetAssignment, DatastreamGroupPartitionsMetadata partitionsMetadata) {
    throw new NotImplementedException("movePartitions are not implemented");
  }

  /**
   * Validate the current assignment and return the list of DatastreamTask per instance to be cleaned up.
   *
   * @param datastreams all the datastreams for which the associated tasks to be validated
   * @param currentAssignment existing assignment
   * @return tasks per instance to be cleaned up
   */
  default Map<String, List<DatastreamTask>> getTasksToCleanUp(List<DatastreamGroup> datastreams,
      Map<String, Set<DatastreamTask>> currentAssignment) {
    return Collections.emptyMap();
  }
}
