/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;


import java.io.IOException;
import java.io.StringWriter;
import java.time.Instant;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;


/**
 * Utility class for converting objects and JSON strings.
 * Exceptions will be logged and the caller is responsible
 * for checking the
 */
public final class JsonUtils {
  private static final Logger LOG = LoggerFactory.getLogger(JsonUtils.class.getName());

  private static final ObjectMapper MAPPER = new ObjectMapper();
  static {
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    MAPPER.addMixIn(Datastream.class, IgnoreDatastreamSetPausedMixIn.class);
    MAPPER.addMixIn(DatastreamSource.class, IgnoreDatastreamSourceSetPartitionsMixIn.class);
    MAPPER.addMixIn(DatastreamDestination.class, IgnoreDatastreamDestinationSetPartitionsMixIn.class);
  }

  /**
   * Deserialize a JSON string into an object with the specified type.
   * @param json JSON string
   * @param clazz class of the target object
   * @param <T> type of the target object
   * @return deserialized Java object
   */
  public static <T> T fromJson(String json, Class<T> clazz) {
    return fromJson(json, clazz, MAPPER);
  }

  /**
   * Deserialize a JSON string into an object with the specified type, using the specified ObjectMapper
   * @param json JSON string
   * @param clazz class of the target object
   * @param mapper the ObjectMapper to use
   * @param <T> type of the target object
   * @return deserialized Java object
   */
  public static <T> T fromJson(String json, Class<T> clazz, ObjectMapper mapper) {
    Validate.notNull(json, "null JSON string");
    Validate.notNull(clazz, "null class object");
    T object = null;
    try {
      object = mapper.readValue(json, clazz);
    } catch (IOException e) {
      String errorMessage = "Failed to parse json: " + json;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
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
    T object = null;
    try {
      object = MAPPER.readValue(json, typeRef);
    } catch (IOException e) {
      String errorMessage = "Failed to parse json: " + json;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
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
      String errorMessage = "Failed to serialize object: " + object;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }
    return out.toString();
  }

  private static abstract class IgnoreDatastreamSourceSetPartitionsMixIn {
    @JsonIgnore
    public abstract DatastreamSource setPartitions(int value);
  }

  private static abstract class IgnoreDatastreamDestinationSetPartitionsMixIn {
    @JsonIgnore
    public abstract DatastreamDestination setPartitions(int value);
  }

  private abstract class IgnoreDatastreamSetPausedMixIn {
    @JsonIgnore
    public abstract void setPaused(Boolean value);
  }

  /**
   * Serializes an Instant object into a Long containing the number of milliseconds from the epoch (Unix time).
   */
  public static class InstantSerializer extends JsonSerializer<Instant> {
    @Override
    public void serialize(Instant value, JsonGenerator generator, SerializerProvider provider) throws IOException {
      generator.writeNumber(value.toEpochMilli());
    }
  }

  /**
   * Serializes an Instant object from a Long containing the number of milliseconds from the epoch (Unix time).
   */
  public static class InstantDeserializer extends JsonDeserializer<Instant> {
    @Override
    public Instant deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      return Instant.ofEpochMilli(parser.getLongValue());
    }
  }
}
