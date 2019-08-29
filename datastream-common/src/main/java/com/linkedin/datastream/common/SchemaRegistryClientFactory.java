/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
  SchemaRegistryClient createSchemaRegistryClient(Properties props);
}
