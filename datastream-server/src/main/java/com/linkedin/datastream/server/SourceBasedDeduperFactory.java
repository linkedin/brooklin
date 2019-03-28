/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Properties;

import com.linkedin.datastream.server.api.connector.DatastreamDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamDeduperFactory;


/**
 * A factory of {@link SourceBasedDeduper} objects
 */
public class SourceBasedDeduperFactory implements DatastreamDeduperFactory {
  @Override
  public DatastreamDeduper createDatastreamDeduper(Properties deduperProperties) {
    return new SourceBasedDeduper();
  }
}
