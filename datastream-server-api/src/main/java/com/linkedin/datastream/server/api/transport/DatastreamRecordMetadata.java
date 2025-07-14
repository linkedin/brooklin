/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

import java.util.List;


/**
 * Metadata of the successfully produced datastream record
 */
public class DatastreamRecordMetadata {

  private final String _topic;
  private final int _partition;
  private final String _checkpoint;
  private final int _eventIndex;
  private final int _sourcePartition;

  // Broadcast record metadata.
  private final boolean _isBroadcastRecord;
  private final List<Integer> _sentToPartitions;
  private final int _partitionCount;
  private final boolean _isMessageSerializationError;

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
    _isBroadcastRecord = false;
    _sentToPartitions = null;
    _partitionCount = -1;
    _isMessageSerializationError = false;
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
    _isBroadcastRecord = false;
    _sentToPartitions = null;
    _partitionCount = -1;
    _isMessageSerializationError = false;
  }

  /**
   * Construct an instance of DatastreamRecordMetadata.
   *
   * @param checkpoint checkpoint string
   * @param topic Kafka topic name
   * @param sentToPartitions List of partitions numbers where send was attempted
   * @param isBroadcastRecord Boolean to indicate if metadata record indicates broadcast
   * @param partitionCount Total number of partitions of the topic
   */
  public DatastreamRecordMetadata(String checkpoint, String topic, List<Integer> sentToPartitions,
      boolean isBroadcastRecord, int partitionCount) {
    _checkpoint = checkpoint;
    _topic = topic;
    _sourcePartition = -1;
    _sentToPartitions = sentToPartitions;
    _eventIndex = 0;
    _isBroadcastRecord = isBroadcastRecord;
    _partition = -1;
    _partitionCount = partitionCount;
    _isMessageSerializationError = false;
  }

  /**
   * Construct an instance of DatastreamRecordMetadata.
   *
   * @param isMessageSerializationError Indicates is serialization error was encountered in EventProducer
   */
  public DatastreamRecordMetadata(boolean isMessageSerializationError) {
    _checkpoint = null;
    _topic = null;
    _sourcePartition = -1;
    _sentToPartitions = null;
    _eventIndex = 0;
    _isBroadcastRecord = true;
    _partition = -1;
    _partitionCount = -1;
    _isMessageSerializationError = isMessageSerializationError;
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
  * Gets the topic name without any decorations or modifications.
  * This base implementation simply returns the topic name as is.
  * Subclasses may override this method to provide custom topic name undecorating logic.
  * @return The undecorated topic name
  */
 public String getUndecoratedTopic() {
   // It is kept for backward compatibility.
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

  public boolean isBroadcastRecord() {
    return _isBroadcastRecord;
  }

  public List<Integer> getSentToPartitions() {
    return _sentToPartitions;
  }

  public int getPartitionCount() {
    return _partitionCount;
  }

  public boolean isMessageSerializationError() {
    return _isMessageSerializationError;
  }
}
