/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang.Validate;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.serde.SerDeSet;


/**
 * List of brooklin events that needs to be sent to the transport provider
 */
public class DatastreamProducerRecord {
  private final Optional<Integer> _partition;
  private final Optional<String> _partitionKey;
  private final Optional<String> _destination;
  private final String _checkpoint;
  private final long _eventsSourceTimestamp;

  private List<BrooklinEnvelope> _events;

  // timestamp of when the record was sent to transport provider
  private Optional<Long> _eventsSendTimestamp = Optional.empty();

  DatastreamProducerRecord(List<BrooklinEnvelope> events, Optional<Integer> partition, Optional<String> partitionKey,
      String checkpoint, long eventsSourceTimestamp) {
    this(events, partition, partitionKey, Optional.empty(), checkpoint, eventsSourceTimestamp);
  }

  DatastreamProducerRecord(List<BrooklinEnvelope> events, Optional<Integer> partition, Optional<String> partitionKey,
      Optional<String> destination, String checkpoint, long eventsSourceTimestamp) {
    Validate.notNull(events, "null event");
    events.forEach((e) -> Validate.notNull(e, "null event"));
    Validate.isTrue(eventsSourceTimestamp > 0, "events source timestamp is invalid");

    _events = events;
    _partition = partition;
    _partitionKey = partitionKey;
    _checkpoint = checkpoint;
    _eventsSourceTimestamp = eventsSourceTimestamp;
    _destination = destination;
  }

  public synchronized void serializeEvents(SerDeSet serDes) {
    _events.forEach(event -> serializeEvent(event, serDes));
  }

  private void serializeEvent(BrooklinEnvelope event, SerDeSet serDes) {
    serDes.getKeySerDe().ifPresent(x -> event.key().ifPresent(k -> event.setKey(x.serialize(k))));
    serDes.getValueSerDe().ifPresent(x -> event.value().ifPresent(v -> event.setValue(x.serialize(v))));

    if (event.getPreviousValue().isPresent() && serDes.getValueSerDe().isPresent()) {
      event.setPreviousValue(serDes.getValueSerDe().get().serialize(event.getPreviousValue()));
    }

    if (serDes.getEnvelopeSerDe().isPresent()) {
      event.setValue(serDes.getEnvelopeSerDe().get().serialize(event));
    }
  }

  /**
   * @return all events in the event record. The events returned can be of type BrooklinEnvelope or a serialized
   * byte array. based on whether the envelope serializer is configured for the stream.
   */
  public synchronized List<BrooklinEnvelope> getEvents() {
    return Collections.unmodifiableList(_events);
  }

  /**
   * @return timestamp in Epoch-millis when the events were produced onto the source
   */
  public long getEventsSourceTimestamp() {
    return _eventsSourceTimestamp;
  }

  public Optional<Long> getEventsSendTimestamp() {
    return _eventsSendTimestamp;
  }

  public void setEventsSendTimestamp(long timestamp) {
    _eventsSendTimestamp = Optional.of(timestamp);
  }

  /**
   * @return destination partition within the destination
   */
  public Optional<Integer> getPartition() {
    return _partition;
  }

  @Override
  public String toString() {
    return String.format("%s @ partitionKey=%s partition=%d", _events, _partitionKey.orElse(null), _partition.orElse(-1));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatastreamProducerRecord record = (DatastreamProducerRecord) o;
    return Objects.equals(_partition, record._partition) && Objects.equals(_partitionKey, record._partitionKey)
        && Objects.equals(_events, record._events) && Objects.equals(_checkpoint, record._checkpoint) &&
        Objects.equals(_destination, record._destination);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_partition, _events, _checkpoint, _destination);
  }

  /**
   * Checkpoint of the event w.r.t source consumption.
   * This is required for checkpoint feature of the
   * event producer.
   *
   * @return string representation of source checkpoint
   */
  public String getCheckpoint() {
    return _checkpoint;
  }

  public Optional<String> getPartitionKey() {
    return _partitionKey;
  }

  /**
   * Normally the destination is defined at the datastream level, but for some scenarios
   * like MirrorMaker, we need to write to multiple topics.
   *
   * If this field is set, then it specify the destination to write this message. If it is
   * missing, then we should it send it to the destination indicated at the datastream level.
   */
  public Optional<String> getDestination() {
    return _destination;
  }
}
