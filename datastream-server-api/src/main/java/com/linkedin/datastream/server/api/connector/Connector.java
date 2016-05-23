package com.linkedin.datastream.server.api.connector;

import java.util.List;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.MetricsAware;
import com.linkedin.datastream.server.DatastreamTask;


/**
 *  Connector interface defines a small set of methods that Coordinator communicates with.
 *  When the Coordinator starts, it will start all connectors it manages by calling the <i>start</i> method. When the Coordinator is
 *  shutting down gracefully, it will stop all connectors by calling the <i>stop</i> method.
 */
public interface Connector extends MetricsAware {

  /**
   * Method to start the connector. This is called immediately after the connector is instantiated.
   * This typically happens when datastream instance starts up.
   */
  void start();

  /**
   * Method to stop the connector. This is called when the datastream instance is being stopped.
   */
  void stop();

  /**
   * callback when the datastreams assignment to this instance is changed. This is called whenever
   * there is a change for the assignment. The implementation of the Connector is responsible
   * to keep a state of the previous assignment.
   *
   * @param tasks the list of the current assignment.
   */
  void onAssignmentChange(List<DatastreamTask> tasks);

  /**
   * Initialize the datastream. Datastream management service call this before writing the
   * Datastream into zookeeper. DMS ensures that stream.source has sufficient details.
   * @param stream: Datastream model
   * @param allDatastreams: all existing datastreams in the system of connector type of the datastream that is being
   *                       initialized.
   * @throws DatastreamValidationException when the datastream that is being created fails any validation.
   */
  void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) throws DatastreamValidationException;

}
