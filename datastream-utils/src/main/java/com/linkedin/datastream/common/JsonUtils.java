package com.linkedin.datastream.common;

import org.apache.commons.lang.Validate;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.StringWriter;

/**
 * Utility class for converting objects and JSON strings.
 * Exceptions will be logged and the caller is responsible
 * for checking the
 */
public final class JsonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class);

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
    ObjectMapper mapper = new ObjectMapper();
    T object;
    try {
      object = mapper.readValue(json, clazz);
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
  public static <T> T fromJson(String json, TypeReference typeRef) {
    Validate.notNull(json, "null JSON string");
    Validate.notNull(typeRef, "null type reference");
    ObjectMapper mapper = new ObjectMapper();
    T object;
    try {
      object = mapper.readValue(json, typeRef);
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
    ObjectMapper mapper = new ObjectMapper();
    StringWriter out = new StringWriter();
    try {
      mapper.writeValue(out, object);
    } catch (IOException e) {
      throw new RuntimeException("Failed to deserialize object: " + object, e);
    }
    return out.toString();
  }
}