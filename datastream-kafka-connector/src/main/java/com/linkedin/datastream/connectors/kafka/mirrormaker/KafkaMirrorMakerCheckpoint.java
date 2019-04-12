/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import org.apache.commons.lang.Validate;


/**
 * Represents a KafkaMirrorMaker source checkpoint in format: topic/partition/offset
 */
public class KafkaMirrorMakerCheckpoint {
  private static final String DELIMITER = "/";
  private String _topic;
  private int _partition;
  private long _offset;

  /**
   * Construct an instance of KafkaMirrorMakerCheckpoint
   * @param topic Kafka topic name
   * @param partition Kafka topic partition
   * @param offset Offset within Kafka topic
   */
  public KafkaMirrorMakerCheckpoint(String topic, int partition, long offset) {
    _topic = topic;
    _partition = partition;
    _offset = offset;
  }

  /**
   * Construct an instance of KafkaMirrorMakerCheckpoint
   * @param checkpoint checkpoint string formatted as topic/partition/offset
   */
  public KafkaMirrorMakerCheckpoint(String checkpoint) {
    Validate.notNull(checkpoint, "Checkpoint cannot be null");
    String[] parts = checkpoint.split(DELIMITER);
    if (parts.length != 3) {
      throw new IllegalArgumentException(
          "Checkpoint should be in format: topic/partition/offset, but found: " + checkpoint);
    }

    _topic = parts[0];
    _partition = Integer.valueOf(parts[1]);
    _offset = Long.valueOf(parts[2]);
  }

  @Override
  public String toString() {
    return _topic + DELIMITER + _partition + DELIMITER + _offset;
  }

  public String getTopic() {
    return _topic;
  }

  public int getPartition() {
    return _partition;
  }

  public long getOffset() {
    return _offset;
  }
}
