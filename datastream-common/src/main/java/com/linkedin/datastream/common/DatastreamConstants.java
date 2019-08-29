/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Various well known datastream constants
 */
public class DatastreamConstants {
  /**
   * Enum indicating type of Datastream Update
   */
  public enum UpdateType {
    // Indicates change in set of paused partitions in datastream.
    PAUSE_RESUME_PARTITIONS
  }
}
