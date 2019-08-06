/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Collections;
import java.util.List;


/**
 * wrap class to store partitions info for a datastream group
 */
public class DatastreamGroupPartitionsMetadata {

  private final DatastreamGroup _datastreamGroup;
  private final List<String> _partitions;

  /**
   * constructor
   * @param datastreamGroup datastream group which handle the partitions
   * @param partitions the partitions that belong to this datastream
   */
  public DatastreamGroupPartitionsMetadata(DatastreamGroup datastreamGroup, List<String> partitions) {
    _datastreamGroup = datastreamGroup;
    _partitions = partitions;
  }

  public DatastreamGroup getDatastreamGroup() {
    return _datastreamGroup;
  }

  public List<String> getPartitions() {
    return Collections.unmodifiableList(_partitions);
  }

  @Override
  public String toString() {
    return String.format("datastream %s, partitions %s", _datastreamGroup.getName(), _partitions);
  }
}
