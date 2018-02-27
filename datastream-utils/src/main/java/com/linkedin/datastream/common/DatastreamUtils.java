package com.linkedin.datastream.common;


import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

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
  private static final String DEFAULT_TOPIC_REUSE = "true";

  /**
   * This type reference will be used when converting paused partitions to/from Json. Paused partitions
   * are stored as part of datastream metadata.
   */
  public static final TypeReference<HashMap<String, Set<String>>> PAUSED_SOURCE_PARTITIONS_JSON_MAP =
      new TypeReference<HashMap<String, Set<String>>>() {
      };

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
   * @param json
   * @return
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
   * @param datastream
   * @return
   */
  public static String toJSON(Datastream datastream) {
    byte[] jsonBytes = DataMapUtils.dataTemplateToBytes(datastream, true);
    return new String(jsonBytes, Charset.defaultCharset());
  }

  public static String getTaskPrefix(Datastream datastream) {
    return datastream.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX);
  }

  public static boolean containsTaskPrefix(Datastream datastream) {
    return datastream.getMetadata().containsKey(DatastreamMetadataConstants.TASK_PREFIX);
  }

  public static Optional<String> getPayloadSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getPayloadSerDe(GetMode.NULL));
  }

  public static Optional<String> getKeySerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getKeySerDe(GetMode.NULL));
  }

  public static Optional<String> getEnvelopeSerDe(Datastream datastream) {
    return Optional.ofNullable(datastream.getDestination()).map(d -> d.getEnvelopeSerDe(GetMode.NULL));
  }

  public static boolean hasValidSource(Datastream stream) {
    return stream.hasSource()
        && stream.getSource().hasConnectionString()
        && !stream.getSource().getConnectionString().isEmpty();
  }

  public static boolean hasValidDestination(Datastream stream) {
    return stream.hasDestination()
        && stream.getDestination().hasConnectionString()
        && !stream.getDestination().getConnectionString().isEmpty()
        && stream.getDestination().hasPartitions()
        && stream.getDestination().getPartitions() > 0;
  }

  public static boolean hasValidOwner(Datastream stream) {
    return stream.hasMetadata()
        && stream.getMetadata().containsKey(DatastreamMetadataConstants.OWNER_KEY)
        && !stream.getMetadata().get(DatastreamMetadataConstants.OWNER_KEY).isEmpty();
  }

  public static boolean isReuseAllowed(Datastream stream) {
    if (!stream.hasMetadata()) {
      return Boolean.parseBoolean(DEFAULT_TOPIC_REUSE);
    } else {
      return Boolean.parseBoolean(
          stream.getMetadata().getOrDefault(REUSE_EXISTING_DESTINATION_KEY, DEFAULT_TOPIC_REUSE));
    }
  }

  public static boolean isUserManagedDestination(Datastream stream) {
    return StringUtils.equals(stream.getMetadata().get(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY),
        Boolean.TRUE.toString());
  }

  public static boolean isConnectorManagedDestination(Datastream stream) {
    return StringUtils.equals(stream.getMetadata().get(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY),
        Boolean.TRUE.toString());
  }

  /**
   * Given a datastream, returns the paused partitions map with format <source, Set<partitions>>
   * @param datastream
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
   * @param sourcePartitions StringMap
   * @return HashMap
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
}
