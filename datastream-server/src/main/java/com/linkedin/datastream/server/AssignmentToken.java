/**
 *  Copyright 2022 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import com.linkedin.datastream.common.JsonUtils;


/**
 * Data structure to store assignment tokens. These are used as a mechanism for followers to signal the leader that
 * they handled assignment change
 */
public final class AssignmentToken {
  private final String _issuedBy;
  private final String _issuedFor;
  private final long  _timestamp;

  /**
   * Constructor for {@link AssignmentToken}
   */
  @JsonCreator
  public AssignmentToken(@JsonProperty("issuedBy") String issuedBy,
      @JsonProperty("issuedFor") String issuedFor,
      @JsonProperty("timestamp") long timestamp) {
    _issuedBy = issuedBy;
    _issuedFor = issuedFor;
    _timestamp = timestamp;
  }

  /**
   * Creates {@link AssignmentToken} instance from JSON
   */
  public static AssignmentToken fromJson(String json) {
    return JsonUtils.fromJson(json, AssignmentToken.class);
  }

  /**
   * Converts the object to JSON
   */
  public String toJson() {
    return JsonUtils.toJson(this);
  }

  /**
   * Gets the name of the leader host that issued the token
   */
  @JsonProperty("issuedBy")
  public String getIssuedBy() {
    return _issuedBy;
  }

  /**
   * Gets the name of the host for which the token was issued
   */
  @JsonProperty("issuedFor")
  public String getIssuedFor() {
    return _issuedFor;
  }

  /**
   * Gets the timestamp (in UNIX epoch format) for when the token was issued
   */
  @JsonProperty("timestamp")
  public long getTimestamp() {
    return _timestamp;
  }
}
