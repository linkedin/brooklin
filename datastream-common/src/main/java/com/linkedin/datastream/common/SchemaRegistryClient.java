package com.linkedin.datastream.common;

import java.io.IOException;
import org.apache.avro.Schema;


public interface SchemaRegistryClient {

  /**
   * Fetch schema by ID. Here is the ID is the hex string of the MD5 hash of the schema string
   *
   * @param id - MD5 hash of the schema id
   * @return The schema object
   */
  public Schema getSchemaByID(String id);

  /**
   * Register schema in the Schema Repository, returns the schema id of the registered schema
   * topic is inter-changeable with subject as required
   *
   * @param topic           The topic name
   * @param schema          The schema object
   * @return MD5 hash of the registered schema's id
   */
  public String registerSchema(String topic, Schema schema) throws IOException;
}
