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
   * Get the Datastream object's task prefix
   * @param datastream datastream object for which to obtain task prefix.
   * @return task prefix string
   */
  public static String getTaskPrefix(Datastream datastream) {
    return datastream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX);
  }

  /**
   * Check whether the Datastream object's task prefix is set.
   * @param datastream datastream object for which to check for task prefix.
   * @return true if task prefix is present; false otherwise.
   */
  public static boolean containsTaskPrefix(Datastream datastream) {
    return datastream.getMetadata().containsKey(DatastreamMetadataConstants.TASK_PREFIX);
  }

  /**
   * Get the Datastream object's payload SerDe if present
   * @param datastream datastream object for which to return payload SerDe
   * @return Optional of Payload SerDe if present.
   */
  public static Optional<String> getPayloadSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getPayloadSerDe(GetMode.NULL));
  }

  /**
   * Get the Datastream object's key SerDe if present
   * @param datastream datastream object for which to return key SerDe
   * @return Optional of Key SerDe if present.
   */
  public static Optional<String> getKeySerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getKeySerDe(GetMode.NULL));
  }

  /**
   * Get the Datastream object's envelope SerDe if present
   * @param datastream datastream object for which to return envelope SerDe
   * @return Optional of Envelope SerDe if present.
   */
  public static Optional<String> getEnvelopeSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getEnvelopeSerDe(GetMode.NULL));
  }

  /**
   * A stream has a valid source if:
   * a) the metadata denotes that the stream has a source
   * b) the source contains a valid connection string which is not empty.
   * @param stream the datastream to validate source for
   * @return true if the conditions above apply; false otherwise.
   */
  public static boolean hasValidSource(Datastream stream) {
    return stream.hasSource()
        && stream.getSource().hasConnectionString()
        && !stream.getSource().getConnectionString().isEmpty();
  }

  /**
   * A stream has a valid destination if:
   * a) the metadata denotes that the stream has a connector-managed destination (so destination info is not required)
   * b) the stream contains non-empty destination connection string and partition count greater than zero
   * @param stream the datastream to validate destination for
   * @return true if the condition above applies; false otherwise
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
   * A stream has a valid owner if:
   * a) the stream metadata exists
   * b) the stream metadata has the owner set such that it is not empty.
   * @param stream the datastream to validate owner for
   * @return true if the conditions above apply; false otherwise.
   */
  public static boolean hasValidOwner(Datastream stream) {
    return stream.hasMetadata()
        && stream.getMetadata().containsKey(DatastreamMetadataConstants.OWNER_KEY)
        && !stream.getMetadata().get(DatastreamMetadataConstants.OWNER_KEY).isEmpty();
  }

  /**
   * Returns whether the datastream should reuse existing datastream's destination if it is available.
   * @param stream the datastream to check for reuse
   * @return true if destination reuse is true; false otherwise.
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
   * Returns whether the datastream has an user managed destination (a.k.a BYOT - Bring your own topic)
   * @param stream the datastream to check for user managed destination.
   * @return true if the datastream has a user managed destination; false otherwise.
   */
  public static boolean isUserManagedDestination(Datastream stream) {
    return StringUtils.equals(stream.getMetadata().get(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY),
        Boolean.TRUE.toString());
  }

  /**
   * Returns whether the datastream has a connector managed destination, so destination should not be created on
   * datastream creation
   * @param stream the datastream to check for connector managed destination.
   * @return true if the datastream has a connector managed destination; false otherwise.
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
