/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.type.TypeReference;

import com.linkedin.data.template.GetMode;
import com.linkedin.data.template.StringMap;
import com.linkedin.restli.internal.server.util.DataMapUtils;

import static com.linkedin.datastream.common.DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY;


/**
 * Simple utility class for working with Datastream-related objects.
 */
public final class DatastreamUtils {
  /**
   * This type reference will be used when converting paused partitions to/from Json. Paused partitions
   * are stored as part of datastream metadata.
   */
  public static final TypeReference<HashMap<String, Set<String>>> PAUSED_SOURCE_PARTITIONS_JSON_MAP =
      new TypeReference<HashMap<String, Set<String>>>() {
      };

  private static final String DEFAULT_TOPIC_REUSE = "true";

  private DatastreamUtils() {
  }

  /**
   * Validate the validity of fields of a newly created Datastream before received
   * and processed by the Coordinator.
   * @param datastream datastream object to be validated
   */
  public static void validateNewDatastream(Datastream datastream) {
    Validate.notNull(datastream, "null datastream");
    Validate.notNull(datastream.getName(GetMode.NULL), "invalid datastream name");
    Validate.notNull(datastream.getConnectorName(GetMode.NULL), "invalid datastream connector type");
    Validate.isTrue(hasValidSource(datastream), "invalid source");
    Validate.isTrue(hasValidOwner(datastream), "missing or invalid owner");
  }

  /**
   * Validate the validity of fields of an existing Datastream.
   * @param datastream datastream object to be validated
   */
  public static void validateExistingDatastream(Datastream datastream) {
    validateNewDatastream(datastream);
    Validate.isTrue(hasValidDestination(datastream), "invalid destination");
  }

  /**
   * Deserialize JSON text into a Datastream object.
   */
  public static Datastream fromJSON(String json) {
    InputStream in = IOUtils.toInputStream(json);
    try {
      Datastream datastream = DataMapUtils.read(in, Datastream.class);
      return datastream;
    } catch (IOException ioe) {
      return null;
    }
  }

  /**
   * Serialize a Datastream object into a JSON text.
   */
  public static String toJSON(Datastream datastream) {
    byte[] jsonBytes = DataMapUtils.dataTemplateToBytes(datastream, true);
    return new String(jsonBytes, Charset.defaultCharset());
  }

  /**
   * Get the task prefix of a Datastream object
   */
  public static String getTaskPrefix(Datastream datastream) {
    return datastream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX);
  }

  /**
   * Get the group name of a Datastream object, it is the same as task prefix
   */
  public static String getGroupName(Datastream datastream) {
    return getTaskPrefix(datastream);
  }

  /**
   * Check if the task prefix of a Datastream object is set
   */
  public static boolean containsTaskPrefix(Datastream datastream) {
    return datastream.getMetadata().containsKey(DatastreamMetadataConstants.TASK_PREFIX);
  }

  /**
   * Get the payload SerDe of a Datastream object, if present
   */
  public static Optional<String> getPayloadSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getPayloadSerDe(GetMode.NULL));
  }

  /**
   * Get the key SerDe of a Datastream object, if present
   */
  public static Optional<String> getKeySerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getKeySerDe(GetMode.NULL));
  }

  /**
   * Get the envelope SerDe of a Datastream object, if present
   */
  public static Optional<String> getEnvelopeSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getEnvelopeSerDe(GetMode.NULL));
  }

  /**
   * Check if a Datastream object has a valid source
   *
   * @param stream the datastream to validate source for
   * @return true if
   * <ol type="a">
   *  <li>a source is specified in the datastream metadata</li>
   *  <li>the specified source contains a non-empty and valid connection string</li>
   * </ol>
   */
  public static boolean hasValidSource(Datastream stream) {
    return stream.hasSource()
        && stream.getSource().hasConnectionString()
        && !stream.getSource().getConnectionString().isEmpty();
  }

  /**
   * Check if a Datastream object has a valid destination
   *
   * @param stream the datastream to validate destination for
   * @return true if
   * <ol type="a">
   *   <li>A connector-managed destination is specified in datastream metadata (destination info is not required)</li>
   *   — OR —
   *   <li>a non-empty connection string is set for destination in datastream metadata</li>
   *   <li>a positive non-zero value is provided for the number of partitions of the specified destination</li>
   * </ol>
   */
  public static boolean hasValidDestination(Datastream stream) {
    return isConnectorManagedDestination(stream)
        || (stream.hasDestination()
        && stream.getDestination().hasConnectionString()
        && !stream.getDestination().getConnectionString().isEmpty()
        && stream.getDestination().hasPartitions()
        && stream.getDestination().getPartitions() > 0);
  }

  /**
   * Check if a Datastream object has a valid owner
   *
   * @param stream the datastream to validate owner for
   * @return true if
   * <ol type="a">
   *  <li>the datastream has non-empty metadata</li>
   *  <li>a non-empty value is specified for the owner field in datastream metadata</li>
   * </ol>
   */
  public static boolean hasValidOwner(Datastream stream) {
    return stream.hasMetadata()
        && stream.getMetadata().containsKey(DatastreamMetadataConstants.OWNER_KEY)
        && !stream.getMetadata().get(DatastreamMetadataConstants.OWNER_KEY).isEmpty();
  }

  /**
   * Check if reusing an existing destination is allowed for a Datastream object
   *
   * @param stream the datastream to check for reuse
   * @return true if datastream has no metadata, or destination reuse is allowed
   */
  public static boolean isReuseAllowed(Datastream stream) {
    if (!stream.hasMetadata()) {
      return Boolean.parseBoolean(DEFAULT_TOPIC_REUSE);
    } else {
      return Boolean.parseBoolean(
          stream.getMetadata().getOrDefault(REUSE_EXISTING_DESTINATION_KEY, DEFAULT_TOPIC_REUSE));
    }
  }

  /**
   * Check if a Datastream object has a user-managed destination (a.k.a BYOT - Bring your own topic)
   */
  public static boolean isUserManagedDestination(Datastream stream) {
    return StringUtils.equals(stream.getMetadata().get(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY),
        Boolean.TRUE.toString());
  }

  /**
   * Check if a Datastream object has a connector-managed destination
   */
  public static boolean isConnectorManagedDestination(Datastream stream) {
    return StringUtils.equals(stream.getMetadata().get(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY),
        Boolean.TRUE.toString());
  }

  /**
   * Given a datastream, returns the paused partitions map with format <source, Set<partitions>>
   * @return map representing paused partitions
   */
  public static Map<String, Set<String>> getDatastreamSourcePartitions(Datastream datastream) {
    // Get the existing set of paused partitions from datastream object.
    // Convert the existing json to map <source, partitions>
    Map<String, Set<String>> sourcePartitionsMap = new HashMap<>();
    String json = datastream.getMetadata().getOrDefault(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, "");

    if (!json.isEmpty()) {
      sourcePartitionsMap = JsonUtils.fromJson(json, DatastreamUtils.PAUSED_SOURCE_PARTITIONS_JSON_MAP);
    }

    return sourcePartitionsMap;
  }

  /**
   * Converts a given StringMap into HashMap
   */
  public static Map<String, Set<String>> parseSourcePartitionsStringMap(StringMap sourcePartitions) {
    HashMap<String, Set<String>> map = new HashMap<>();
    for (String source : sourcePartitions.keySet()) {
      String[] values = sourcePartitions.get(source).split(",");
      HashSet<String> partitions = new HashSet<>(Arrays.asList(values));
      map.put(source, partitions);
    }
    return map;
  }

  /**
   * Given a list of datastreams, returns the set of unique metadata group IDs from those datastreams.
   */
  public static Set<String> getMetadataGroupIDs(Collection<Datastream> datastreams) {
    return datastreams.stream()
        .filter(Datastream::hasMetadata)
        .map(ds -> ds.getMetadata().get(DatastreamMetadataConstants.GROUP_ID))
        .filter(Objects::nonNull)
        .collect(Collectors.toSet());
  }
}
