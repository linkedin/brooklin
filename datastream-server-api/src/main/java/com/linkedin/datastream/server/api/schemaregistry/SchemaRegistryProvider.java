package com.linkedin.datastream.server.api.schemaregistry;

import org.apache.avro.Schema;


/**
 * Schemaregistry provider.
 */
public interface SchemaRegistryProvider {

  /**
   * Register the schema in schema registry. If the schema already exists in the registry
   * Just return the schema Id of the existing
   * @param schemaName Name of the schema. Schema within the same name needs to be backward compatible.
   * @param schema Schema that needs to be registered.
   * @return
   *   SchemaId of the registered schema.
   * @throws SchemaRegistryException if the register schema fails.
   */
  String registerSchema(String schemaName, Schema schema) throws SchemaRegistryException;
}
