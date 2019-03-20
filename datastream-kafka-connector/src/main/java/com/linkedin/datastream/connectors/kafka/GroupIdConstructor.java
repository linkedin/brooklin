/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.security.MessageDigest;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.xml.bind.DatatypeConverter;

import org.slf4j.Logger;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.server.DatastreamTask;


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
   * The method, given task prefix and cluster name, returns group ID with format
   * clusterName + Hash(taskPrefix). This will be used as default later once grandfathering of existing datastreams
   * is done.
   */
  default String constructGroupId(String taskPrefix, String clusterName) {
    return clusterName + "." + hashGroupId(taskPrefix);
  }

  /**
   * The method is supposed to populate group ID in a given datastream's metadata.
   * Default implementation uses following precedence to populate group ID:
   * 1. If group ID is specified already in  datastream's metadata, use it as it is.
   * 2. If group ID is overridden in metadata in other duplicate datastream, use that group ID.
   * 3. If group ID isn't found yet, then construct it using constructGroupId method and populate it in metadata.
   * @param datastream Datastream for which group ID needs to be populated
   * @param allDatastreams All datastreams belonging to connector. They will be used to find duplicate datastreams.
   * @param logger - optional logger in case steps while populating datastream group ID should be recorded.
   */
  default void populateDatastreamGroupIdInMetadata(Datastream datastream, List<Datastream> allDatastreams,
      Optional<Logger> logger) {
    String datastreamTaskPrefix = DatastreamUtils.getTaskPrefix(datastream);
    List<Datastream> existingDatastreamsWithGroupIdOverride = allDatastreams.stream()
        .filter(DatastreamUtils::containsTaskPrefix)
        .filter(ds -> DatastreamUtils.getTaskPrefix(ds) == datastreamTaskPrefix)
        .filter(ds -> ds.getMetadata().containsKey(DatastreamMetadataConstants.GROUP_ID))
        .collect(Collectors.toList());

    String groupId;

    // if group ID is specified in metadata, use it directly
    if (datastream.getMetadata().containsKey(DatastreamMetadataConstants.GROUP_ID)) {
      groupId = datastream.getMetadata().get(DatastreamMetadataConstants.GROUP_ID);
      logger.ifPresent(log -> log.info("Datastream {} has group ID specified in metadata. Will use that ID: {}",
          datastream.getName(), groupId));
    } else if (!existingDatastreamsWithGroupIdOverride.isEmpty()) {
      // if existing datastream has group ID in it already, copy it over.
      groupId = existingDatastreamsWithGroupIdOverride.get(0).getMetadata().get(DatastreamMetadataConstants.GROUP_ID);
      logger.ifPresent(
          log -> log.info("Found existing datastream {} for datastream {} with group ID. Copying its group id: {}",
              existingDatastreamsWithGroupIdOverride.get(0).getName(), datastream.getName(), groupId));
    } else {
      // else create and and keep it in metadata.
      groupId = constructGroupId(datastream);
      logger.ifPresent(
          log -> log.info("Constructed group ID for datastream {}. Group id: {}", datastream.getName(), groupId));
    }
    datastream.getMetadata().put(DatastreamMetadataConstants.GROUP_ID, groupId);
  }

  /**
   * The method is supposed to return group ID for a task.
   * Default implementation:
   * 1. Checks if group ID is present in any datastream's metadata and returns it if found. (It also checks for
   * group ID inconsistency)
   * 2. If group ID is not found, it constructs it explicitly and returns it.
   * Note: If some group ID constructor doesn't get group ID in any other way other than explicitly constructing it,
   * this method should be same as constructGroupId(DatastreamTask task) method.
   * @param task Task for which group ID should be returned
   * @param logger Optional logger - in case logs should be recorded while getting group ID
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
      logger.ifPresent(log -> log.info(
          "Found overridden group ID for Kafka datastream task: {} . Overridden group id: {} Datastreams: {}",
          task.getId(), groupIds.toArray()[0], task.getDatastreams()));
      return (String) groupIds.toArray()[0];
    } else {
      String groupId = constructGroupId(task);
      logger.ifPresent(log -> log.info("Constructed group ID: {} for task: {}", groupId, task.getId()));
      return groupId;
    }
  }

  /**
   * Hashes given group ID using MD5.
   * @param groupId - Group ID to hash
   * @return Hashed group ID string
   */
  default public String hashGroupId(String groupId) {
    try {
      MessageDigest digest = MessageDigest.getInstance("MD5");
      byte[] hashedBytes = digest.digest(groupId.getBytes("UTF-8"));
      return DatatypeConverter.printHexBinary(hashedBytes).toLowerCase();
    } catch (Exception e) {
      throw new DatastreamRuntimeException(
          String.format("Can't hash group ID.Group ID: %s. Exception: %s", groupId, e));
    }
  }
}
