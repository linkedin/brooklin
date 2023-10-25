/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.linkedin.datastream.common.LogUtils;

/**
 * wrap class to store partitions info for a datastream group
 */
public class DatastreamGroupPartitionsMetadata {

  private final DatastreamGroup _datastreamGroup;
  private final Set<String> _partitions;

  /**
   * constructor
   * @param datastreamGroup datastream group which handle the partitions
   * @param partitions the partitions in a list that belong to this datastream
   */
  public DatastreamGroupPartitionsMetadata(DatastreamGroup datastreamGroup, List<String> partitions) {
    this(datastreamGroup, new HashSet<>(partitions));
  }

  /**
   * constructor
   * @param datastreamGroup datastream group which handle the partitions
   * @param partitions the partitions in a set that belong to this datastream
   */
  public DatastreamGroupPartitionsMetadata(DatastreamGroup datastreamGroup, Set<String> partitions) {
    _datastreamGroup = datastreamGroup;
    _partitions = partitions;
  }

  public DatastreamGroup getDatastreamGroup() {
    return _datastreamGroup;
  }

  public Set<String> getPartitions() {
    return Collections.unmodifiableSet(_partitions);
  }

  @Override
  public String toString() {
    return String.format("datastream %s, partitions %s", _datastreamGroup.getName(),
        LogUtils.logSummarizedTopicPartitionsMapping(new ArrayList<>(_partitions)));
  }
}
