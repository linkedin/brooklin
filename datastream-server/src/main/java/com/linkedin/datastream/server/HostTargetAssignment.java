/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.List;

import com.linkedin.datastream.common.JsonUtils;


/**
 * Data structure to store the target assignment for partitions for a particular host
 */
public class HostTargetAssignment {
  private List<String> _partitionNames;
  private String _targetHost;

  /**
   * Constructor for HostTargetAssignment
   */
  public HostTargetAssignment(List<String> partitionName, String targetHost) {
    _partitionNames = partitionName;
    _targetHost = targetHost;
  }

  /**
   * Constructor for HostTargetAssignment, required for json
   */
  public HostTargetAssignment() {

  }

  /**
   * create HostTargetAssignment from json
   */
  public static HostTargetAssignment fromJson(String json) {
    HostTargetAssignment assignment = JsonUtils.fromJson(json, HostTargetAssignment.class);
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
