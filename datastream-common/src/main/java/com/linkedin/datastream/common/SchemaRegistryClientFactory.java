package com.linkedin.datastream.common;

import java.util.Properties;


/**
 * Interface for the schema registry factory
 */
public interface SchemaRegistryClientFactory {

  /**
   * Create an instance of a Schema Registry Client
   *
   * @param props - properties containing information such as registry URI and mode
   * @return SchemaRegistryClient implementation
   */
  public SchemaRegistryClient createSchemaRegistryClient(Properties props);
}
