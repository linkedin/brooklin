/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.connector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 *  Connector interface defines a small set of methods that Coordinator communicates with.
 *  When the Coordinator starts, it will start all connectors it manages by calling the <i>start</i> method. When the
 *  Coordinator is shutting down gracefully, it will stop all connectors by calling the <i>stop</i> method.
 */
public interface Connector extends MetricsAware, DatastreamChangeListener {
  /**
   * Method to start the connector.
   * This is called immediately after the connector is instantiated. This typically happens when brooklin server starts up.
   * @param checkpointProvider CheckpointProvider if the connector needs a checkpoint store.
   */
  void start(CheckpointProvider checkpointProvider);

  /**
   * Method to stop the connector. This is called when the brooklin server is being stopped.
   */
  void stop();

  /**
   * Callback when the datastreams assigned to this instance is changed. The implementation of the Connector is
   * responsible to keep a state of the previous assignment.
   * @param tasks the list of the current assignment.
   */
  void onAssignmentChange(List<DatastreamTask> tasks);

  /**
   * Initialize the datastream. Any connector-specific validations for the datastream needs to performed here.
   * Connector can mutate the datastream object in this call. Framework will write the updated datastream object.
   * NOTE:
   * <ol>
   *   <li>
   *     This method is called by the Rest.li service before the datastream is written to ZooKeeper, so please make sure,
   *     this call doesn't block for more then few seconds otherwise the REST call will time out.</li>
   *   <li>
   *     It is possible that the brooklin framework may call this method in parallel to another onAssignmentChange
   *     call. It is up to the connector to perform the synchronization if it needs between initialize and onAssignmentChange.</li>
   * </ol>
   * @param stream Datastream model
   * @param allDatastreams all existing datastreams in the system of connector type of the datastream that is being
   *                       initialized.
   * @throws DatastreamValidationException when the datastream that is being created fails any validation.
   */
  void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) throws DatastreamValidationException;

  /**
   * Returns a list with IDs for tasks that are active (i.e. running or pending stop)
   * @return A list with task IDs that are currently running on the coordinator
   * @throws UnsupportedOperationException if not implemented by Connector classes.
   */
  default List<String> getActiveTasks() {
    throw new UnsupportedOperationException("Active tasks API is not supported unless implemented by connectors");
  }

  /**
   * Validate the update datastreams operation. By default this is not supported. Any connectors that want to support
   * datastream updates should override this method to perform the validation needed.
   * @param datastreams list of datastreams to be updated
   * @param allDatastreams all existing datastreams in the system of connector type of the datastream that is being
   *                       validated.
   * @throws DatastreamValidationException when the datastreams update is not valid
   */
  default void validateUpdateDatastreams(List<Datastream> datastreams, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    throw new DatastreamValidationException("Datastream update is not supported");
  }

  /**
   * Checks if certain update type is supported by given connector. Can be used to validate if given update operation is
   * supported for given connector(s).
   * @param datastream datastream to check for update support
   * @param updateType Type of datastream update
   */
  default boolean isDatastreamUpdateTypeSupported(Datastream datastream, DatastreamConstants.UpdateType updateType) {
    return false;
  }

  /**
   * Returns whether Brooklin should manage partition assignment
   *
   * Brooklin allows the partitions to be managed and assigned by the upstream source (ex. Kafka) or
   * Brooklin itself. This tells if Brooklin should manage the partition for this connector
   *
   * @return
   *  true if the connector relies on Brooklin to manage partitions
   *  false if this connector relies on the source (ex. Kafka) to manage and assign partitions
   */
  default boolean isPartitionManagementSupported() {
    return false;
  }

  /**
   * Compute the topic name, the default implement is based on datastream name and current time.
   * @param datastream the current assignment.
   */
  default String getDestinationName(Datastream datastream) {
    String datastreamName = datastream.getName();
    String uid = datastream.getMetadata().get(DatastreamMetadataConstants.UID);
    // if uid is set, append this to the datastreamName
    if (!StringUtils.isBlank(uid)) {
      return String.format("%s_%s", datastreamName, uid);
    }
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
    LocalDateTime localDateTime = null;
    String createTime = datastream.getMetadata().get(DatastreamMetadataConstants.CREATION_MS);
    if (!StringUtils.isEmpty(createTime)) {
      // convert createTime to localDateTime and append it to the datastream name.
      localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(createTime)), ZoneOffset.UTC);
    } else {
      localDateTime = LocalDateTime.now();
    }
    String currentTime = formatter.format(localDateTime);
    return String.format("%s_%s", datastreamName, currentTime);
  }

  /**
   * Hook that can be used to do any final connector related initializations on datastream, after coordinator is done
   * with its set of initializations.
   *
   * NOTE: This method is called by the Rest.li service before the datastream is written to ZooKeeper, so please make
   *   sure this call doesn't block for more than few seconds otherwise the REST call will timeout.
   * @param stream Datastream being initialized
   * @param allDatastreams all existing datastreams in the system of connector type of the datastream that is being
   *                       initialized.
   */
  default void postDatastreamInitialize(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
  }

  /**
   * Hook that can be used to do any additional operations once the datastream has been created, update or deleted
   * successfully on the ZooKeeper. This method will be invoked for datastream state change too.
   *
   * NOTE: This method is called by the Rest.li service after the datastream is written to/deleted from ZooKeeper. So
   * please make sure this call doesn't block for more than few seconds otherwise the REST call will timeout. If you
   * have non-critical work, see if that can be done as an async operation or on a separate thread.
   * @param stream the datastream
   * @throws DatastreamException on fail to perform post datastream state change operations successfully.
   */
  default void postDatastreamStateChangeAction(Datastream stream) throws DatastreamException {
  }
}
