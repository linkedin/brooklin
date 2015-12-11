package com.linkedin.datastream.server.api.schemaregistry;

import java.util.Properties;


/**
 * SchemaRegistry provider factory that is used to create the schema registry provider instances.
 */
public interface SchemaRegistryProviderFactory {

  public SchemaRegistryProvider createSchemaRegistryProvider(Properties properties);
}
