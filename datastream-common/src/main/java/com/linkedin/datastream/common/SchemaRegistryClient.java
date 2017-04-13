package com.linkedin.datastream.common;

import org.apache.avro.Schema;


public interface SchemaRegistryClient {

  /**
   * Fetch schema by ID. Here is the ID is the hex string of the MD5 hash of the schema string
   *
   * @param id - MD5 hash of the schema id
   * @return The schema object
   */
  public Schema getSchemaByID(String id);
}
