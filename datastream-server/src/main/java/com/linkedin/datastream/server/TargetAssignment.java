/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;

import com.linkedin.datastream.common.JsonUtils;


/**
 * Data structure to store the target assignment for partitions
 */
public class TargetAssignment {
  private List<String> _partitionNames;
  private String _targetHost;

  /**
   * Constructor for TargetAssignment
   */
  public TargetAssignment(List<String> partitionName, String targetHost) {
    _partitionNames = partitionName;
    _targetHost = targetHost;
  }

  /**
   * Constructor for TargetAssignment, required for json
   */
  public TargetAssignment() {

  }

  /**
   * create TargetAssignment from json
   */
  public static TargetAssignment fromJson(String json) {
    TargetAssignment assignment = JsonUtils.fromJson(json, TargetAssignment.class);
    return assignment;
  }

  /**
   * convert this object to json
   */
  public String toJson() {
    return JsonUtils.toJson(this);
  }


  public String getTargetHost() {
    return _targetHost;
  }

  public List<String> getPartitionNames() {
    return _partitionNames;
  }

  public void setPartitionNames(List<String> partitionNames) {
    _partitionNames = partitionNames;
  }

  public void setTargetHost(String targetHost) {
    _targetHost = targetHost;
  }
}
