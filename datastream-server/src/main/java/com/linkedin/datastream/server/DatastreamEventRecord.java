package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamEvent;

import org.apache.commons.lang.StringUtils;

/**
 * Envelope of a Datastream event to be sent via Kafka.
 */
public class DatastreamEventRecord {
  private final String _topic;
  private final Integer _partition;
  private final Object _key;
  private final DatastreamEvent _event;

  public DatastreamEventRecord(String topic, Integer partition, Object key, DatastreamEvent event) {
    if (StringUtils.isEmpty(topic) || partition < 0 || key == null || event == null)
      throw new IllegalArgumentException();
    _topic = topic;
    _partition = partition;
    _key = key;
    _event = event;
  }

  /**
   * @return target Kafka topic name.
   */
  public String topic() {
    return _topic;
  }

  /**
   * @return topic partition number
   */
  public Integer partition() {
    return _partition;
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
