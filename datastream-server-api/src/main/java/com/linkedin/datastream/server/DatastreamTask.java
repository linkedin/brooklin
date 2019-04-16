/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.serde.SerDeSet;


/**
 * Describes a unit of work generated from a {@link Datastream}. Every {@link Datastream} is broken
 * down into smaller units of work, DatastreamTasks, that are assigned to one or more
 * {@link com.linkedin.datastream.server.api.connector.Connector} instances and processed concurrently.
 * @see com.linkedin.datastream.server.api.strategy.AssignmentStrategy
 */
public interface DatastreamTask {

  /**
   * Get the connector type that generates events for this datastream task.
   */
  String getConnectorType();

  /**
   * Get the name of the transport provider associated with the datastream task.
   */
  String getTransportProviderName();

  /**
   * Get the event producer that the connector can use to produce events for this datastream task.
   */
  DatastreamEventProducer getEventProducer();

  /**
   * Get the Id of the datastream task. Each datastream task will have a unique id.
   */
  String getId();

  /**
   * The Connector implementation can use this method to obtain the last saved state.
   * @param key for which the state needs to be returned.
   * @return the last saved state corresponding to the key.
   */
  String getState(String key);

  /**
   * Connectors can store the state associated with the task.
   * State is associated with a key.
   * @param key Key to which the state needs to be associated with.
   * @param value Actual state that needs to be stored.
   */
  void saveState(String key, String value);

  /**
   * Serdes used to serialize the events in the destination
   */
  SerDeSet getDestinationSerDes();

  /**
   * Get the current status of the datastream task.
   */
  DatastreamTaskStatus getStatus();

  /**
   * Set the status of the datastream task.
   * This is a way for the connector implementation to persist the status of the datastream task
   * @param status Status of the datastream task.
   */
  void setStatus(DatastreamTaskStatus status);

  /**
   * Get the name of the datastream task.
   */
  String getDatastreamTaskName();

  /**
   * Get whether the destination is user-managed.
   */
  boolean isUserManagedDestination();

  /**
   * Get the datastream source.
   */
  DatastreamSource getDatastreamSource();

  /**
   * Get the datastream destination.
   */
  DatastreamDestination getDatastreamDestination();

  /**
   * Get the list of partitions this task covers.
   */
  List<Integer> getPartitions();

  /**
   * Get a map of safe checkpoints which are guaranteed
   * to have been flushed onto the transport. Key is partition
   * number, and value is the safe checkpoint for it.
   */
  Map<Integer, String> getCheckpoints();

  /**
   * Get the task prefix that is used to identify all the tasks corresponding to the datastream group.
   */
  String getTaskPrefix();

  /**
   * Get the list of datastreams for which this task is producing events.
   */
  List<Datastream> getDatastreams();

  /**
   * A connector must acquire a task before starting to process it. This ensures no
   * two instances will work on the same task concurrently thus causing duplicate
   * events. This can happen in task reassignment induced by new or dead instances.
   * This is a no-op if the task is already acquired by the same instance.
   * @param timeout duration to wait before timeout in acquiring the task
   */
  void acquire(Duration timeout);

  /**
   * A connector should remember to release a task if the task is unassigned to it
   * such that the next assigned instance can acquire the task for processing.
   * This is a no-op if the task is not assigned to the current instance.
   */
  void release();
}
