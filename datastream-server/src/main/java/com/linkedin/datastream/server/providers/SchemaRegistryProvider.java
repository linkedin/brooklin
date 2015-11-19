package com.linkedin.datastream.server.providers;

import org.apache.avro.Schema;


/**
 * Schemaregistry provider.
 */
public interface SchemaRegistryProvider {

  /**
   * Register the schema in schema registry. If the schema already exists in the registry
   * Just return the schema Id of the existing
   * @param schema Schema that needs to be registered.
   * @return
   *   SchemaId of the registered schema.
   */
  String registerSchema(Schema schema);
}
