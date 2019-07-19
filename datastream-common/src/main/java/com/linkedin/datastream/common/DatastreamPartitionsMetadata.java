/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.Collections;
import java.util.List;


/**
 * wrap class to store partitions info for a datastreamv group
 */
public class DatastreamPartitionsMetadata {

  private final String _datastreamGroupName;
  private final List<String> _partitions;

  /**
   * constructor
   * @param datastreamGroupName name of the datastream group
   * @param partitions the partitions that subscribed by this datastream
   */
  public DatastreamPartitionsMetadata(String datastreamGroupName, List<String> partitions) {
    _datastreamGroupName = datastreamGroupName;
    _partitions = partitions;
  }

  public String getDatastreamGroupName() {
    return _datastreamGroupName;
  }

  public List<String> getPartitions() {
    return Collections.unmodifiableList(_partitions);
  }

  @Override
  public String toString() {
    return String.format("datastream %s, partitions %s", _datastreamGroupName, _partitions);
  }
}
