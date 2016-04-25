package com.linkedin.datastream.server.api.transport;

/**
 * Metadata of the succesfully produced datastream record
 */
public class DatastreamRecordMetadata {

  private final String _topic;
  private final int _partition;
  private final String _checkpoint;

  public DatastreamRecordMetadata(String checkpoint, String topic, int partition) {
    _checkpoint = checkpoint;
    _topic = topic;
    _partition = partition;
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
}
