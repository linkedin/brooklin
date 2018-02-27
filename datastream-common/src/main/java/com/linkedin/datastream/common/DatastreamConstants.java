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
    PAUSE_RESUME_PARTITIONS;
  }
}
