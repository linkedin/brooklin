/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.connector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;

import com.linkedin.datastream.common.DatastreamPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamGroup;


/**
 * Datastream Listener defines the interface requires for management the changes inside datastream, such as partitions.
 * It is required to implement this interface if the connector wants to manage the partition assignment
 */
public interface DatastreamChangeListener {

  /**
   * register a consumer function which will be triggered when a partition change is detected within this connector
   *
   * The callback function is a lambda consumer takes the name of datastream group as a consumer. It is expected to
   * be issued in the same thread that detecting the partition changed. Thus we expect this callback function to
   * be finished very quickly
   *
   * @param callback a lamda consumer which takes the datastream group
   */
  default void onPartitionChange(Consumer<DatastreamGroup> callback) {

  }

  /**
   * callback when the datastreamGroups assigned to this connector instance has been changed
   */
  default void handleDatastream(List<DatastreamGroup> datastreamGroups) {

  }

  /**
   * Get the partitions for all datastream group. Return Optional.empty() for that datastreamGroup if the
   * datastreamGroup has been assigned but the partition info has not been fetched already
   */
  default Map<String, Optional<DatastreamPartitionsMetadata>> getDatastreamPartitions() {
    return new HashMap<String, Optional<DatastreamPartitionsMetadata>>();
  }
}
