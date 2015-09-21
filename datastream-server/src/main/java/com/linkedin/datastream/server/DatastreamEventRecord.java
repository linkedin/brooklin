package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamEvent;

/**
 * Envelope of a Datastream event to be sent via Kafka.
 */
public class DatastreamEventRecord {
  private final Datastream _datastream;
  private final Object _key;
  private final DatastreamEvent _event;

  public DatastreamEventRecord(Datastream datastream, Object key, DatastreamEvent event) {
    if (datastream == null || key == null || event == null)
      throw new IllegalArgumentException();
    _datastream = datastream;
    _key = key;
    _event = event;
  }

  /**
   * @return Datastream object
   */
  public Datastream datastream() {
    return _datastream;
  }

  /**
   * @return Kafka message key
   */
  public Object key() {
    return _key;
  }

  /**
   * @return Datastream event object
   */
  public DatastreamEvent event() {
    return _event;
  }
}
