package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamEvent;

import java.util.Objects;


/**
 * Envelope of a Datastream event to be sent via Kafka.
 */
public class DatastreamEventRecord {
  private final int _partition;
  private final DatastreamEvent _event;

  public DatastreamEventRecord(DatastreamEvent event, int partition) {
    Objects.requireNonNull(event, "invalid event");
    if (partition < 0) {
      throw new IllegalArgumentException("invalid partition.");
    }

    _event = event;
    _partition = partition;
  }

  /**
   * @return Datastream event object
   */
  public DatastreamEvent event() {
    return _event;
  }

  /**
   * @return destination partition within the topic
   */
  public int partition() {
    return _partition;
  }

  @Override
  public String toString() {
    return String.format("%s @ part=%d", _event, _partition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    DatastreamEventRecord record = (DatastreamEventRecord) o;
    return Objects.equals(_partition, record._partition) && Objects.equals(_event, record._event);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_partition, _event);
  }
}
