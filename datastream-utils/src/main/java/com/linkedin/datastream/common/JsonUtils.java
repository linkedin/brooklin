package com.linkedin.datastream.common;

import java.io.IOException;
import java.io.StringWriter;
import org.apache.commons.lang.Validate;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;


/**
 * Utility class for converting objects and JSON strings.
 * Exceptions will be logged and the caller is responsible
 * for checking the
 */
public final class JsonUtils {
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    final DeserializationConfig config = MAPPER.getDeserializationConfig();
    config.addMixInAnnotations(DatastreamSource.class, IgnoreDatastreamSourceSetPartitionsMixIn.class);
    config.addMixInAnnotations(DatastreamDestination.class, IgnoreDatastreamDestinationSetPartitionsMixIn.class);
  }

  private static abstract class IgnoreDatastreamSourceSetPartitionsMixIn {
    @JsonIgnore
    public abstract DatastreamSource setPartitions(int value);
  }

  private static abstract class IgnoreDatastreamDestinationSetPartitionsMixIn {
    @JsonIgnore
    public abstract DatastreamDestination setPartitions(int value);
  }

  /**
   * Deserialize a JSON string into an object with the specified type.
   * @param json JSON string
   * @param clazz class of the target object
   * @param <T> type of the target object
   * @return deserialized Java object
   */
  public static <T> T fromJson(String json, Class<T> clazz) {
    Validate.notNull(json, "null JSON string");
    Validate.notNull(clazz, "null class object");
    T object;
    try {
      object = MAPPER.readValue(json, clazz);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse json: " + json, e);
    }
    return object;
  }

  /**
   * Deserialize a JSON string into an object based on a type reference.
   * This method allows the caller to specify precisely the desired output
   * type for the target object.
   * @param json JSON string
   * @param typeRef type reference of the target object
   * @param <T> type of the target object
   * @return deserialized Java object
   */
  public static <T> T fromJson(String json, TypeReference<T> typeRef) {
    Validate.notNull(json, "null JSON string");
    Validate.notNull(typeRef, "null type reference");
    T object;
    try {
      object = MAPPER.readValue(json, typeRef);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse json: " + json, e);
    }
    return object;
  }

  /**
   * Serialize a Java object into JSON string.
   * @param object object to be serialized
   * @param <T> type of the input object
   * @return JSON string
   */
  public static <T> String toJson(T object) {
    Validate.notNull(object, "null input object");
    StringWriter out = new StringWriter();
    try {
      MAPPER.writeValue(out, object);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize object: " + object, e);
    }
    return out.toString();
  }
}
