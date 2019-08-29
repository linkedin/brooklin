/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.io.IOException;

import org.apache.avro.Schema;


/**
 * Abstraction of a Kafka schema registry client.
 */
public interface SchemaRegistryClient {

  /**
   * Fetch schema by ID. Here is the ID is the hex string of the MD5 hash of the schema string
   *
   * @param id - MD5 hash of the schema id
   * @return The schema object
   */
  Schema getSchemaByID(String id);

  /**
   * Register schema in the Schema Repository, returns the schema id of the registered schema
   * topic is inter-changeable with subject as required
   *
   * @param topic           The topic name
   * @param schema          The schema object
   * @return MD5 hash of the registered schema's id
   */
  String registerSchema(String topic, Schema schema) throws IOException;
}
