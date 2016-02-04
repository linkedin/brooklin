package com.linkedin.datastream.server;

import org.apache.avro.Schema;

import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryException;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;


public class MockSchemaRegistryProvider implements SchemaRegistryProvider {

  public static String MOCK_SCHEMA_ID = "mockSchemaId";

  @Override
  public String registerSchema(Schema schema)
      throws SchemaRegistryException {
    return MOCK_SCHEMA_ID;
  }
}
