package com.linkedin.datastream.connectors.kafka;

import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class MockDatastreamEventProducer implements DatastreamEventProducer {

  private final List<DatastreamProducerRecord> events = Collections.synchronizedList(new ArrayList<>());
  private int numFlushes = 0;

  @Override
  public void send(DatastreamProducerRecord event, SendCallback callback) {
    events.add(event);
    DatastreamRecordMetadata md = new DatastreamRecordMetadata(event.getCheckpoint(), "mock topic", 666);
    callback.onCompletion(md, null);
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
}
