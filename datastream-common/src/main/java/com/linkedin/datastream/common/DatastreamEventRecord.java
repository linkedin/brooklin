package com.linkedin.datastream.common;

import java.util.Objects;


/**
 * Envelope of a Datastream event to be sent via Kafka.
 */
public class DatastreamEventRecord {
  private final int _partition;
  private final String _topicName;
  private final DatastreamEvent _event;

  // Event properties for Producer
  private final boolean _isTxnEnd;
  private final int _eventSize;
  private final String _sourceOffset;

  public DatastreamEventRecord(DatastreamEvent event, String topicName, int partition,
                               boolean isTxnEnd, int eventSize, String sourceOffset) {
    Objects.requireNonNull(event, "invalid event");
    if (partition < 0) {
      throw new IllegalArgumentException("invalid partition.");
    }

    _event = event;
    _topicName = topicName;
    _partition = partition;
    _isTxnEnd = isTxnEnd;
    _eventSize = eventSize;
    _sourceOffset = sourceOffset;
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
  public int getPartition() {
    return _partition;
  }

  /**
   * @return destination topic name.
   */
  public String getTopicName() {
    return _topicName;
  }

  @Override
  public String toString() {
    return String.format("%s @ part=%d", _event, _partition);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    DatastreamEventRecord record = (DatastreamEventRecord) o;
    return Objects.equals(_partition, record._partition) &&
            Objects.equals(_isTxnEnd, record._isTxnEnd) &&
            Objects.equals(_eventSize, record._eventSize) &&
            Objects.equals(_topicName, record._topicName) &&
            Objects.equals(_event, record._event) &&
            Objects.equals(_sourceOffset, record._sourceOffset);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_partition, _topicName, _event, _isTxnEnd, _eventSize, _sourceOffset);
  }

  /**
   * @return if the current event is the end of a transaction
   */
  public boolean isTxnEnd() {
    return _isTxnEnd;
  }

  /**
   * @return the byte size of the event
   */
  public int getEventSize() {
    return _eventSize;
  }

  /**
   * Offset of the event in the context of source consumption.
   * This is a mandatory field for connectors utilizing the
   * common checkpoint framework provided by EventProducer.
   * For connectors with custom checkpointing, this field is
   * optional.
   *
   * @return string representation of source offset
   */
  public String getSourceOffset() {
    return _sourceOffset;
  }

}
