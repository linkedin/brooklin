package com.linkedin.datastream.server;

import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.server.api.transport.SendCallback;


/**
 * Implementation of the DatastremaEventProducer that connector will use to produce events. There is an unique
 * DatastreamEventProducerImpl object created per DatastreamTask that is assigned to the connector.
 * DatastreamEventProducer will inturn use a shared EventProducer (shared across the tasks that use the same destinations)
 * to produce the events.
 */
public class DatastreamEventProducerImpl implements DatastreamEventProducer {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamEventProducerImpl.class);

  private EventProducer _eventProducer;
  private final DatastreamTask _task;

  public DatastreamEventProducerImpl(DatastreamTask task, EventProducer eventProducer) {
    _eventProducer = eventProducer;
    _task = task;
  }

  void resetEventProducer(EventProducer eventProducer) {
    _eventProducer = eventProducer;
  }

  EventProducer getEventProducer() {
    return _eventProducer;
  }

  @Override
  public void send(DatastreamProducerRecord event, SendCallback sendCallback) {
    _eventProducer.send(_task, event, sendCallback);
  }

  @Override
  public void flush() {
    _eventProducer.flushAndCheckpoint();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DatastreamEventProducerImpl producer = (DatastreamEventProducerImpl) o;
    return Objects.equals(_eventProducer, producer._eventProducer) &&
        Objects.equals(_task, producer._task);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_eventProducer, _task);
  }
}
