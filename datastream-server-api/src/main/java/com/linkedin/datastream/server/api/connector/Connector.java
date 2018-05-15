package com.linkedin.datastream.server.api.connector;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.providers.CheckpointProvider;


/**
 *  Connector interface defines a small set of methods that Coordinator communicates with.
 *  When the Coordinator starts, it will start all connectors it manages by calling the <i>start</i> method. When the
 *  Coordinator is shutting down gracefully, it will stop all connectors by calling the <i>stop</i> method.
 */
public interface Connector extends MetricsAware {

  /**
   * Method to start the connector. This is called immediately after the connector is instantiated.
   * This typically happens when brooklin server starts up.
   */
  void start();

  /**
   * Method to start the connector.
   * This is called immediately after the connector is instantiated. This typically happens when brooklin server starts up.
   *
   * This API will eventually replace the no argument version and is meant to help manage dependent modules not breaking
   * from not implementing this API. Once all modules have moved to this version of the API, the other API will be removed
   * and default implementation removed.
   *
   * @param checkpointProvider DatastreamCheckpointProvider if the connector needs a checkpoint store.
   */
  default void start(CheckpointProvider checkpointProvider) {
    start();
  }

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
   * Initialize the datastream. Any connector specific validations for the datastream needs to performed here.
   * Connector can mutate the datastream object in this call. Framework will write the updated datastream object.
   * NOTE:
   *   1. This method is called by the rest li service before the datastream is written to the zookeeper, So please make sure,
   *   this call doesn't block for more then few seconds otherwise the rest call will timeout.
   *   2. It is possible that the brooklin framework may call this method in parallel to another onAssignmentChange
   *   call. It is upto the connector to perform the synchronization if it needs between initialize and onAssignmentChange.
   * @param stream: Datastream model
   * @param allDatastreams: all existing datastreams in the system of connector type of the datastream that is being
   *                       initialized.
   * @throws DatastreamValidationException when the datastream that is being created fails any validation.
   */
  void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) throws DatastreamValidationException;

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
   * @param updateType Type of datastream update
   * @throws DatastreamValidationException when connector doesn't support update type.
   */
  default boolean isDatastreamUpdateTypeSupported(Datastream datastream, DatastreamConstants.UpdateType updateType) {
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

}
