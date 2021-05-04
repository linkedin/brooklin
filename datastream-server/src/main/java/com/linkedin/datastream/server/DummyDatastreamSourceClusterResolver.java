/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

/**
 * A dummy implementation for {@link DatastreamSourceClusterResolver}
 */
public class DummyDatastreamSourceClusterResolver implements DatastreamSourceClusterResolver {
  @Override
  public String getSourceCluster(DatastreamGroup datastreamGroup) {
    return "";
  }
}
