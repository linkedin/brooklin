package com.linkedin.datastream.connectors.oracle.triggerbased;



import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;

import com.linkedin.datastream.common.SchemaRegistryClient;
import com.linkedin.datastream.common.AvroMessageEncoderUtil;

public class MockSchemaRegistry implements SchemaRegistryClient {
  private static final Map<String, Schema> ID_TO_SCHEMA_MAP = new ConcurrentHashMap<>();
  private static final Map<String, List<Schema>> TOPIC_TO_SCHEMAS_MAP = new ConcurrentHashMap<>();

  public MockSchemaRegistry() {
  }

  public Schema getSchemaByID(String id) {
    Schema schema = ID_TO_SCHEMA_MAP.get(id);
    if (schema == null) {
      throw new RuntimeException("Schema does not exist for id " + id);
    }
    return schema;
  }

  public String registerSchema(String topic, Schema schema) {
    String id = AvroMessageEncoderUtil.schemaToHex(schema);
    List<Schema> schemaHistory = TOPIC_TO_SCHEMAS_MAP.get(topic);

    if (schemaHistory == null) {
      schemaHistory = new LinkedList<Schema>();
      TOPIC_TO_SCHEMAS_MAP.put(topic, schemaHistory);
    }

    // For simplicity we don't check whether the schema is backward compatible
    if (!schemaHistory.contains(schema)) {
      schemaHistory.add(schema);
    }

    ID_TO_SCHEMA_MAP.put(id, schema);
    return id;
  }
}
