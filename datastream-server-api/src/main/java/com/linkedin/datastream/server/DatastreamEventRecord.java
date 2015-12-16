package com.linkedin.datastream.server;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang.Validate;

import com.linkedin.datastream.common.DatastreamEvent;


/**
 * Envelope of a Datastream event to be sent via Kafka.
 */
public class DatastreamEventRecord {
  private final DatastreamTask _task;
  private final int _partition;
  private final String _checkpoint;
  private final List<DatastreamEvent> _events;

  public DatastreamEventRecord(DatastreamEvent event, int partition, String checkpoint, DatastreamTask task) {
    this(Arrays.asList(new DatastreamEvent[] {event}), partition, checkpoint, task);
  }

  public DatastreamEventRecord(List<DatastreamEvent> events, int partition, String checkpoint, DatastreamTask task) {
    Validate.notEmpty(checkpoint, "empty checkpoint");
    Validate.notNull(events, "null event");
    Validate.notNull(task, "null task");

    // TODO: partition can be negative magic number for special meaning, eg. re-partitioning.
    // For now, we requires partition to be non-negative.
    Validate.isTrue(partition >= 0, "invalid partition number: " + String.valueOf(partition));

    events.forEach((e) -> Validate.notNull(e, "null event"));

    _events = events;
    _partition = partition;
    _checkpoint = checkpoint;
    _task = task;
  }

  /**
   * @return all events in the event record
   */
  public List<DatastreamEvent> getEvents() {
    return Collections.unmodifiableList(_events);
  }

  /**
   * @return destination partition within the destination
   */
  public int getPartition() {
    return _partition;
  }

  /**
   * @return destination name.
   */
  public String getDestination() {
    return _task.getDatastreamDestination().getConnectionString();
  }

  /**
   * @return DatastreamTask this event is being produced for.
   */
  public DatastreamTask getDatastreamTask() {
    return _task;
  }

  @Override
  public String toString() {
    return String.format("%s @ task=%s, partition=%d", _events, _task, _partition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatastreamEventRecord record = (DatastreamEventRecord) o;
    return Objects.equals(_task, record._task) &&
           Objects.equals(_partition, record._partition) &&
           Objects.equals(_events, record._events) &&
           Objects.equals(_checkpoint, record._checkpoint);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_task, _partition, _events, _checkpoint);
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

}
