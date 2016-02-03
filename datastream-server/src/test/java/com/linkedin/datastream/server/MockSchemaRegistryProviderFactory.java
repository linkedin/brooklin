package com.linkedin.datastream.server;

import java.util.Properties;

import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProvider;
import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryProviderFactory;


public class MockSchemaRegistryProviderFactory implements SchemaRegistryProviderFactory {

  @Override
  public SchemaRegistryProvider createSchemaRegistryProvider(Properties properties) {
    return new MockSchemaRegistryProvider();
  }
}


