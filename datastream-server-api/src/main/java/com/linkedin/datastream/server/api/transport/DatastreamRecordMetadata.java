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
  private final int _eventIndex;
  private final int _sourcePartition;

  /**
   * Construct an instance of DatastreamRecordMetadata. Defaults the event index to 0 and source partition to -1.
   * @param  checkpoint checkpoint string
   * @param topic Kafka topic name
   * @param partition Destination Kafka topic partition
   */
  public DatastreamRecordMetadata(String checkpoint, String topic, int partition) {
    _checkpoint = checkpoint;
    _topic = topic;
    _partition = partition;
    _eventIndex = 0;
    _sourcePartition = -1;
  }

  /**
   * Construct an instance of DatastreamRecordMetadata.
   * @param checkpoint checkpoint string
   * @param topic Kafka topic name
   * @param partition Destination Kafka topic partition
   * @param eventIndex Index of event within {@link com.linkedin.datastream.server.DatastreamProducerRecord}
   * @param sourcePartition Source Kafka topic partition
   */
  public DatastreamRecordMetadata(String checkpoint, String topic, int partition, int eventIndex, int sourcePartition) {
    _checkpoint = checkpoint;
    _topic = topic;
    _partition = partition;
    _eventIndex = eventIndex;
    _sourcePartition = sourcePartition;
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
   * An index identifying the exact {@link com.linkedin.datastream.common.BrooklinEnvelope} event produced,
   * from those obtainable through {@link com.linkedin.datastream.server.DatastreamProducerRecord#getEvents()}
   */
  public int getEventIndex() {
    return _eventIndex;
  }

  /**
   * Partition number from which the record was consumed.
   */
  public int getSourcePartition() {
    return _sourcePartition;
  }

  @Override
  public String toString() {
    return String.format("Checkpoint: %s, Topic: %s, Destination Partition: %d, Event Index: %d, Source Partition: %d",
        _checkpoint, _topic, _partition, _eventIndex, _sourcePartition);
  }
}
