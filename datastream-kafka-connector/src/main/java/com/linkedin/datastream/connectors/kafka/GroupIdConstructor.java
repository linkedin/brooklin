package com.linkedin.datastream.connectors.kafka;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;

public interface GroupIdConstructor {

  /**
   * The method, given a datastream, is supposed to construct group ID for it and return it.
   * @param datastream Datastream for which group ID should be constructed.
   * @return constructed group ID
   */
  String constructGroupId(Datastream datastream);

  /**
   * The method, given a datastream task, is supposed to construct group ID for it and return it.
   * @param datastreamTask DatastreamTask for which group ID should be constructed.
   * @return constructed group ID
   */
  String constructGroupId(DatastreamTask datastreamTask);

  /**
   * The method is supposed to populate group ID in a given datastream's metadata.
   * Default implementation uses following precedence to populate group ID:
   * 1. If group ID is specified already in  datastream's metadata, use it as it is.
   * 2. If group ID is overridden in metadata in other duplicate datastream, use it as it is to populate given datastream's metadata.
   * 3. If group ID isn't found yet, then construct it using constructGroupId method and populate it in metadata.
   * @param datastream Datastream for which group ID needs to be populated
   * @param allDatastreams All datastreams belonging to connector. They will be used to find duplicate datastreams.
   * @param deduper Deduper to use to find out existing duplicate datastream
   * @param logger - optional logger in case steps while populating datastream group ID should be recorded.
   * @throws DatastreamValidationException when deduper detects any invalidity of the new stream
   */
  default void populateDatastreamGroupIdInMetadata(Datastream datastream, List<Datastream> allDatastreams,
      DatastreamDeduper deduper, Optional<Logger>  logger)
      throws DatastreamValidationException {
    Optional<Datastream> existingDatastream = deduper.findExistingDatastream(datastream, allDatastreams);

    String groupId;

    // if group ID is specified in metadata, use it directly
    if (datastream.getMetadata().containsKey(DatastreamMetadataConstants.GROUP_ID)) {
      groupId = datastream.getMetadata().get(DatastreamMetadataConstants.GROUP_ID);
      if (logger.isPresent()) {
        logger.get().info("Datastream {} has group ID specified in metadata. Will use that ID: {}", datastream.getName(),
            groupId);
      }
    } else if (existingDatastream.isPresent()
        && existingDatastream.get().getMetadata().containsKey(DatastreamMetadataConstants.GROUP_ID)) {
      // if existing datastream has group ID in it already, copy it over.
      groupId = existingDatastream.get().getMetadata().get(DatastreamMetadataConstants.GROUP_ID);
      if (logger.isPresent()) {
        logger.get().info("Found existing datastream {} for datastream {} with group ID. Copying its group id: {}",
            existingDatastream.get().getName(), datastream.getName(), groupId);
      }
    } else {
      // else create and and keep it in metadata.
      groupId = constructGroupId(datastream);
      if (logger.isPresent()) {
        logger.get().info("Constructed group ID for datastream {}. Group id: {}", datastream.getName(), groupId);
      }
    }
    datastream.getMetadata().put(DatastreamMetadataConstants.GROUP_ID, groupId);
  }

  /**
   * The method is supposed to return group ID for a task.
   * Default implementation:
   * 1. Checks if group ID is present in any datastream's metadata and return it if found. (It also checks for
   * group ID inconsistency
   * 2. If group ID not found, it constructs it explicitly and returns it.
   * Note: If some group ID constructor doesn't get group ID in any other way other than explicitly constructing it,
   * this method should be same as constuctGroupId(DatastreamTask task) method.
   * @param task Task for which group ID should be returned
   * @param logger Optional logger - in case logs should be recorded while getting group ID
   * @return
   */
  default String getTaskGroupId(DatastreamTask task, Optional<Logger> logger) {
    Set<String> groupIds = DatastreamUtils.getMetadataGroupIDs(task.getDatastreams());
    if (!groupIds.isEmpty()) {
      if (groupIds.size() != 1) {
        String errMsg =
            String.format("Found multiple consumer group ids for connector task: %s. Group IDs: %s. Datastreams: %s",
                task.getId(), groupIds, task.getDatastreams());
        throw new DatastreamRuntimeException(errMsg);
      }
      // if group ID is present in metadata, add it to properties even if present already.
      if (logger.isPresent()) {
        logger.get().info("Found overridden group ID for Kafka datastream task: {} . Overridden group id: {} Datastreams: %s",
            task.getId(), groupIds.toArray()[0], task.getDatastreams());
      }
      return (String) groupIds.toArray()[0];
    } else {
      String groupId = constructGroupId(task);
      if (logger.isPresent()) {
        logger.get().info(String.format("Constructed group ID: %s for task: %s", groupId, task.getId()));
      }
      return groupId;
    }
  }
}
