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


public interface DatastreamTask {

  /**
   * @return the connector type that generates events for this datastream task.
   */
  String getConnectorType();

  /**
   * @return the name of the transport provider associated with the datastream task.
   */
  String getTransportProviderName();

  /**
   * @return the event producer that the connector can use to produce events for this datastream task.
   */
  DatastreamEventProducer getEventProducer();

  /**
   * @return Id of the datastream task. Each datastream task will have a unique id.
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
   * @return Current status of the datastream task.
   */
  DatastreamTaskStatus getStatus();

  /**
   * Set the status for DatastreamTask. This is a way for the connector
   * implementation to persist the status of the datastream task
   * @param status Status of the datastream task.
   */
  void setStatus(DatastreamTaskStatus status);

  /**
   * @return the name of the datastream task.
   */
  String getDatastreamTaskName();

  /**
   * @return whether the destination is a user managed.
   */
  boolean isUserManagedDestination();

  /**
   * @return the Datastream source.
   */
  DatastreamSource getDatastreamSource();

  /**
   * @return the Datastream destination.
   */
  DatastreamDestination getDatastreamDestination();

  /**
   * @return the list of partitions this task covers.
   */
  List<Integer> getPartitions();

  /**
   * @return a map of safe checkpoints which are guaranteed
   * to have been flushed onto the transport. Key is partition
   * number, and value is the safe checkpoint for it.
   */
  Map<Integer, String> getCheckpoints();

  /**
   * Get the task prefix that is used to identify all the tasks corresponding to the datastream group.
   */
  String getTaskPrefix();

  /**
   * @return the list of datastreams for which this task is producing events for.
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
