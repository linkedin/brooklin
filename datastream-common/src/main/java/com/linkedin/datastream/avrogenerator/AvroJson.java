/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.base.CaseFormat;


/**
 * This is a helper class to build all the components of the
 * AvroSchema in the form of a HashMap.
 *
 * The final to {@code #toSchema()} will convert that Avro Json
 * into a real Avro.Schema instance.
 */
public class AvroJson {
  private static final String NAME_KEY = "name";
  private static final String TYPE_KEY = "type";
  private static final String ARRAY_ITEMS_KEY = "items";
  private static final String DEFAULT_KEY = "default";
  private static final String FIELD_KEY = "fields";
  private static final String META_KEY = "meta";
  private static final String DOC_KEY = "doc";
  private static final String NAMESPACE_KEY = "namespace";

  private static final String ARRAY_TYPE = "array";
  private static final String RECORD_TYPE = "record";

  private final Map<String, Object> _info = new HashMap<>();

  /**
   * A static helper method to build a inner arrayType Avro Json
   */
  public static AvroJson arrayType(String dbFieldTypeName, AvroJson items) {
    AvroJson avroRecord = new AvroJson();

    avroRecord.setType(ARRAY_TYPE);
    avroRecord.setName(CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, dbFieldTypeName));
    avroRecord.setArrayItems(items.info().get(TYPE_KEY));

    return avroRecord;
  }

  /**
   * A static helper method to build an inner recordType Avro Json
   */
  public static AvroJson recordType(String name, String metadata) {
    AvroJson avroRecord = new AvroJson();

    avroRecord.setName(name);
    avroRecord.setMeta(metadata);
    avroRecord.setType(RECORD_TYPE);

    return avroRecord;
  }

  /**
   * A simple helper method that wraps an Avro type around an array and adds "null"
   * @param type The avro Collection or Struct Type
   * @return ["null", { avroType }]
   */
  public static List<Object> nullableType(AvroJson type) {
    List<Object> nullableType = new ArrayList<>();
    nullableType.add(type.info());
    nullableType.add("null");

    return nullableType;
  }

  public void setName(String name) {
    _info.put(NAME_KEY, name);
  }

  public void setMeta(String meta) {
    _info.put(META_KEY, meta);
  }

  public void setDoc(String doc) {
    _info.put(DOC_KEY, doc);
  }

  public void setType(Object type) {
    _info.put(TYPE_KEY, type);
  }

  public void setArrayItems(Object items) {
    _info.put(ARRAY_ITEMS_KEY, items);
  }

  public void setNamespace(String namespace) {
    _info.put(NAMESPACE_KEY, namespace);
  }

  public void nullDefault() {
    _info.put(DEFAULT_KEY, null);
  }

  public void setFields(List<Map<String, Object>> fields) {
    _info.put(FIELD_KEY, fields);
  }

  public Map<String, Object> info() {
    return _info;
  }

  /**
   * Convert the HashMap {@code _info} into an AvroSchema
   * This involves using the Avro Parser which will ensure the entire
   * schema is properly formatted
   *
   * If the Schema is not formatted properly, this function will throw an
   * error
   */
  public Schema toSchema() throws SchemaGenerationException {
    try {
      ObjectMapper mapper = new ObjectMapper();
      JsonFactory factory = new JsonFactory();
      StringWriter writer = new StringWriter();
      JsonGenerator jgen = factory.createJsonGenerator(writer);
      jgen.useDefaultPrettyPrinter();
      mapper.writeValue(jgen, _info);
      String schema = writer.getBuffer().toString();

      return Schema.parse(schema);
    } catch (IOException e) {
      throw new SchemaGenerationException("Failed to generate Schema from AvroJson", e);
    }
  }
}
