/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.connector;

import java.util.Properties;


/**
 * Factory for DatastreamDeduper
 */
public interface DatastreamDeduperFactory {

  /**
   * Create datastream deduper
   * @param deduperProperties properties for datastream deduper
   */
  DatastreamDeduper createDatastreamDeduper(Properties deduperProperties);
}
