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
  private final String _checkpoint;
  private List<BrooklinEnvelope> _events;
  private final long _eventsSourceTimestamp;

  DatastreamProducerRecord(List<BrooklinEnvelope> events, Optional<Integer> partition, Optional<String> partitionKey,
      String checkpoint, long eventsSourceTimestamp) {
    Validate.notNull(events, "null event");
    events.forEach((e) -> Validate.notNull(e, "null event"));
    Validate.isTrue(eventsSourceTimestamp > 0, "events source timestamp is invalid");
    Validate.isTrue(partition.isPresent() || partitionKey.isPresent(),
        "Either partition or partitionKey should be present");

    _events = events;
    _partition = partition;
    _partitionKey = partitionKey;
    _checkpoint = checkpoint;
    _eventsSourceTimestamp = eventsSourceTimestamp;
  }

  public synchronized void serializeEvents(SerDeSet serDes) {
    _events.stream().forEach(event -> serializeEvent(event, serDes));
  }

  private void serializeEvent(BrooklinEnvelope event, SerDeSet serDes) {
    serDes.getKeySerDe().ifPresent(x -> event.setKey(x.serialize(event.getKey())));
    serDes.getValueSerDe().ifPresent(x -> event.setValue(x.serialize(event.getValue())));
    serDes.getValueSerDe().ifPresent(x -> event.setPreviousValue(x.serialize(event.getPreviousValue())));

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

  /**
   * @return destination partition within the destination
   */
  public Optional<Integer> getPartition() {
    return _partition;
  }

  @Override
  public String toString() {
    if (_partition.isPresent()) {
      return String.format("%s @ partition=%s", _events, _partition.get());
    } else {
      return String.format("%s @ partitionKey=%s", _events, _partitionKey.get());
    }
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
        && Objects.equals(_events, record._events) && Objects.equals(_checkpoint, record._checkpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_partition, _events, _checkpoint);
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
}
