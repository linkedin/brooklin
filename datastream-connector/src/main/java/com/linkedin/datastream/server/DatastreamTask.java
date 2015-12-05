package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;


public interface DatastreamTask {

  /**
   * @return the connector type that generates events for this datastream task.
   */
  public String getConnectorType();

  /**
   * @return the event producer that the connector can use to produce events for this datastream task.
   */
  public EventProducer getEventProducer();

  /**
   * @return Id of the datastream task. Each datastream task will have a unique id.
   */
  public String getId();

  /**
   * The Connector implementation can use this method to obtain the last saved state.
   * @param key for which the state needs to be returned.
   * @return  the last saved state corresponding to the key.
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
   * Set the status for datastreamtask. This is a way for the connector
   * implementation to persist the status of the datastream task
   * @param status Status of the datastream task.
   */
  void setStatus(DatastreamTaskStatus status);

  /**
   * @return Current status of the datastream task.
   */
  DatastreamTaskStatus getStatus();

  /**
   * @return the name of the datastream task.
   */
  String getDatastreamTaskName();

  /**
   * @return the Datastream source.
   */
  DatastreamSource getDatastreamSource();

  /**
   * @return the Datastream destination.
   */
  DatastreamDestination getDatastreamDestination();
}
