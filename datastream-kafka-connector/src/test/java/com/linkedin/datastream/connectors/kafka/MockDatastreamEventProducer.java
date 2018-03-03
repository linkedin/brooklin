package com.linkedin.datastream.connectors.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;


public class MockDatastreamEventProducer implements DatastreamEventProducer {

  private static final Logger LOG = LoggerFactory.getLogger(MockDatastreamEventProducer.class);
  private final List<DatastreamProducerRecord> events = Collections.synchronizedList(new ArrayList<>());
  private int numFlushes = 0;
  private Predicate<DatastreamProducerRecord> _sendFailCondition;

  public MockDatastreamEventProducer() {
    this((record) -> false);
  }

  public MockDatastreamEventProducer(Predicate<DatastreamProducerRecord> sendFailCondition) {
    _sendFailCondition = sendFailCondition;
  }

  @Override
  public void send(DatastreamProducerRecord event, SendCallback callback) {
    if (_sendFailCondition.test(event)) {
      throw new DatastreamRuntimeException("Random exception");
    }

    events.add(event);
    LOG.info("sent event {} , total events {}", event, events.size());
    DatastreamRecordMetadata md = new DatastreamRecordMetadata(event.getCheckpoint(), "mock topic", 666);
    if (callback != null) {
      callback.onCompletion(md, null);
    }
  }

  @Override
  public void flush() {
    numFlushes++;
  }

  public List<DatastreamProducerRecord> getEvents() {
    return events;
  }

  public int getNumFlushes() {
    return numFlushes;
  }

  public void updateSendFailCondition(Predicate<DatastreamProducerRecord> sendFailCondition) {
    _sendFailCondition = sendFailCondition;
  }
}
