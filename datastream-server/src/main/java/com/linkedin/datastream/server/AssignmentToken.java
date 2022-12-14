/**
 *  Copyright 2022 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import com.linkedin.datastream.common.JsonUtils;


/**
 * Data structure to store assignment tokens. These are used as a mechanism for followers to signal the leader that
 * they handled assignment change
 */
public final class AssignmentToken {
  private String _issuedBy;
  private String _issuedFor;
  private long  _timestamp;

  /**
   * Constructor for {@link AssignmentToken}
   */
  public AssignmentToken(String issuedBy, String issuedFor) {
    _issuedBy = issuedBy;
    _issuedFor = issuedFor;
    _timestamp = System.currentTimeMillis();
  }

  /**
   * Default constructor for {@link AssignmentToken}, required for json ser/de
   */
  public AssignmentToken() {

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
  public String getIssuedBy() {
    return _issuedBy;
  }

  /**
   * Gets the name of the host for which the token was issued
   */
  public String getIssuedFor() {
    return _issuedFor;
  }

  /**
   * Gets the timestamp (in UNIX epoch format) for when the token was issued
   */
  public long getTimestamp() {
    return _timestamp;
  }

  /**
   * Sets the name of the leader host that issued the token
   */
  public void setIssuedBy(String issuedBy) {
    _issuedBy = issuedBy;
  }

  /**
   * Sets the name of the host for which the token was issued
   */
  public void setIssuedFor(String issuedFor) {
    _issuedFor = issuedFor;
  }

  /**
   * Sets the timestamp (in UNIX epoch format) for when the token was issued
   */
  public void setTimestamp(long timestamp) {
    _timestamp = timestamp;
  }
}
