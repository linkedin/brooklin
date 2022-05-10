/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;


/**
 * Builder class for DatastreamProducerRecord
 */
public class DatastreamProducerRecordBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamProducerRecordBuilder.class.getName());

  private Optional<Integer> _partition = Optional.empty();
  private String _sourceCheckpoint = "";
  private final List<BrooklinEnvelope> _events = new ArrayList<>();
  private long _eventsSourceTimestamp;
  private Optional<String> _partitionKey = Optional.empty();
  private Optional<String> _destination = Optional.empty();
  private boolean _isBroadcastRecord = false;

  /**
   * Partition to which this DatastreamProducerRecord should be produced. If the partition is not set, TransportProvider
   * is expected to use key to send the event to appropriate partition. In this case, It is possible that
   * events within the DatastreamProducerRecord will be sent to different partitions.
   */
  public void setPartition(int partition) {
    Validate.isTrue(partition >= 0, "invalid partition number: " + partition);
    _partition = Optional.of(partition);
  }

  /**
   * Set partition key
   */
  public void setPartitionKey(String partitionKey) {
    Validate.notEmpty(partitionKey, "partitionKey cannot be empty.");
    _partitionKey = Optional.of(partitionKey);
  }

  /**
   * Set destination
   */
  public void setDestination(String destination) {
    Validate.notEmpty(destination, "destination cannot be empty.");
    _destination = Optional.of(destination);
  }

  public void setSourceCheckpoint(String sourceCheckpoint) {
    _sourceCheckpoint = sourceCheckpoint;
  }

  /**
   * Add the event with key and value to the DatastreamProducerRecord. Datastream producer record can have multiple events.
   */
  public void addEvent(Object key, Object value, Object previousValue, Map<String, String> metadata) {
    key = key == null ? new byte[0] : key;
    value = value == null ? new byte[0] : value;
    _events.add(new BrooklinEnvelope(key, value, previousValue, metadata));
  }

  /**
   * Add a message to the events in the DatastreamProducerRecord
   */
  public void addEvent(BrooklinEnvelope envelope) {
    _events.add(envelope);
  }

  /**
   * Set events source timestamp
   */
  public void setEventsSourceTimestamp(long eventsSourceTimestamp) {
    _eventsSourceTimestamp = eventsSourceTimestamp;
  }

  public void setIsBroadcastRecord(boolean isBroadcastRecord) {
    _isBroadcastRecord = isBroadcastRecord;
  }

  /**
   * Build the DatastreamProducerRecord.
   * @return
   *   DatastreamProducerRecord that is created.
   */
  public DatastreamProducerRecord build() {
    return new DatastreamProducerRecord(_events, _partition, _partitionKey, _destination, _sourceCheckpoint,
        _eventsSourceTimestamp, _isBroadcastRecord);
  }

  /**
   * Create DatastreamProducerRecord copied from another record and overriding the partition number
   *
   * @param record datastream record to be copied
   * @param partition partition to override
   * @return copiedDatastreamProducerRecord
   */
  public static DatastreamProducerRecord copyProducerRecord(DatastreamProducerRecord record, int partition) {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    record.getEvents().forEach(builder::addEvent);
    builder.setPartition(partition);
    if (record.getPartitionKey().isPresent()) {
      builder.setPartitionKey(record.getPartitionKey().get());
    }
    if (record.getDestination().isPresent()) {
      builder.setDestination(record.getDestination().get());
    }
    builder.setSourceCheckpoint(record.getCheckpoint());
    builder.setEventsSourceTimestamp(record.getEventsSourceTimestamp());
    builder.setIsBroadcastRecord(record.isBroadcastRecord());
    return builder.build();
  }
}
