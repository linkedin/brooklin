/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

import java.time.Duration;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * Transport provider Admin interface that each of the transport providers needs to implement.
 * The TransportProviderAdmin handles the control path operations of the TransportProvider, allowing
 * the TransportProvider itself to manage actual data movement.
 *
 * Some task handled by the TransportProviderAdmin:
 * - Datastream destination initialization
 * - Assignment/unassignment of DatastreamTasks to the appropriate TransportProvider objects
 * - Creation/deletion of the destination for the datastream
 * - Obtaining retention duration of the destination
 */
public interface TransportProviderAdmin extends MetricsAware {

  /**
   * Assign the instance of the TransportProvider to the DatastreamTask. TransportProviderAdmin can
   * choose to create a new TransportProvider to reuse the existing transport provider for this task.
   * @return
   *   TransportProvider associated with the task
   */
  TransportProvider assignTransportProvider(DatastreamTask task);

  /**
   * Method notifies the TransportProviderAdmin that the datastream Task is no longer using the
   * Transport provider. Admin can decide to close the TransportProvider if none of the DatastreamTasks are
   * using the Transport provider.
   * @param task DatastreamTask that is not using the transport provider.
   */
  void unassignTransportProvider(DatastreamTask task);

  /**
   * Initializes the destination for the datastream.
   * If the datastream has destination already filled. Transport provider performs the necessary validations.
   * @param datastream
   *   Datastream whose destination needs to be validated.
   * @param destinationName
   *   destinationName that are computed by the connector and needed to set the ConnectionString
   */
  void initializeDestinationForDatastream(Datastream datastream, String destinationName) throws DatastreamValidationException;

  /**
   * Create the destination for the datastream.
   */
  void createDestination(Datastream datastream);

  /**
   * Drop the destination for the datastream.
   * @param datastream whose destination needs to be dropped.
   */
  void dropDestination(Datastream datastream);

  /**
   * Query the retention duration of a specific destination.
   * @param datastream datastream whose retention needs to be found.
   * @return retention duration
   */
  Duration getRetention(Datastream datastream);
}
