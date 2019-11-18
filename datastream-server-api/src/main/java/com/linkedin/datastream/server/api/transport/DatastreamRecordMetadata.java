/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

/**
 * Metadata of the successfully produced datastream record
 */
public class DatastreamRecordMetadata {

  private final String _topic;
  private final int _partition;
  private final String _checkpoint;
  private final int _brooklinEventIndex;

  /**
   * Construct an instance of DatastreamRecordMetadata. Defaults the record index to 0.
   * @param  checkpoint checkpoint string
   * @param topic Kafka topic name
   * @param partition Kafka topic partition
   */
  public DatastreamRecordMetadata(String checkpoint, String topic, int partition) {
    _checkpoint = checkpoint;
    _topic = topic;
    _partition = partition;
    _brooklinEventIndex = 0;
  }

  /**
   * Construct an instance of DatastreamRecordMetadata.
   * @param checkpoint checkpoint string
   * @param topic Kafka topic name
   * @param partition Kafka topic partition
   * @param brooklinEventIndex Index of event within {@link com.linkedin.datastream.server.DatastreamProducerRecord}
   */
  public DatastreamRecordMetadata(String checkpoint, String topic, int partition, int brooklinEventIndex) {
    _checkpoint = checkpoint;
    _topic = topic;
    _partition = partition;
    _brooklinEventIndex = brooklinEventIndex;
  }

  /**
   * Source checkpoint of the produced record.
   */
  public String getCheckpoint() {
    return _checkpoint;
  }

  /**
   * Partition number to which the record was produced to.
   */
  public int getPartition() {
    return _partition;
  }

  /**
   * Topic to which the record was produced to
   */
  public String getTopic() {
    return _topic;
  }

  /**
   * Event index within the {@link com.linkedin.datastream.server.DatastreamProducerRecord}'s event list
   */
  public int getBrooklinEventIndex() {
    return _brooklinEventIndex;
  }

  @Override
  public String toString() {
    return String.format("Checkpoint: %s, Topic: %s, Partition: %d, Event Index: %d", _checkpoint, _topic, _partition,
        _brooklinEventIndex);
  }
}
