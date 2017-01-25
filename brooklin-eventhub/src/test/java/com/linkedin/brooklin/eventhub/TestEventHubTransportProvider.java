package com.linkedin.brooklin.eventhub;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.DatastreamEvent;
import com.linkedin.datastream.common.DatastreamEventMetadata;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;


public class TestEventHubTransportProvider {

  private static final Logger LOG = LoggerFactory.getLogger(TestEventHubTransportProvider.class.getName());

  @Test
  public void testSend() {
    EventHubDestination destination = createEventHubDestination();
    EventHubTransportProvider tp = new EventHubTransportProvider(destination, Collections.singletonList(0));
    DatastreamProducerRecord record = createProducerRecord();
    tp.send(TestEventHubTransportProviderAdmin.DESTINATION, record, (metadata, exception) -> LOG.info("Send complete"));
  }

  private DatastreamProducerRecord createProducerRecord() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(createDatastreamEvent("key1", "value1"));
    builder.addEvent(createDatastreamEvent("key2", "value2"));
    builder.setPartition(0);
    builder.setSourceCheckpoint("cp1");
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    return builder.build();
  }

  private DatastreamEvent createDatastreamEvent(String key, String value) {
    DatastreamEvent event = new DatastreamEvent();
    event.payload = ByteBuffer.wrap(value.getBytes());
    event.key = ByteBuffer.wrap(key.getBytes());
    event.metadata = new HashMap<>();
    long currentTimeMillis = System.currentTimeMillis();
    event.metadata.put(DatastreamEventMetadata.EVENT_TIMESTAMP, String.valueOf(currentTimeMillis));
    event.previous_payload = ByteBuffer.allocate(0);
    return event;
  }

  private EventHubDestination createEventHubDestination() {
    return new EventHubDestination(TestEventHubTransportProviderAdmin.createEventHubDatastream("test1"));
  }

  @Test
  public void testFlush() {
    EventHubDestination destination = createEventHubDestination();
    EventHubTransportProvider tp = new EventHubTransportProvider(destination, Collections.singletonList(0));
    DatastreamProducerRecord record = createProducerRecord();
    tp.send(TestEventHubTransportProviderAdmin.DESTINATION, record, (metadata, exception) -> LOG.info("Send complete"));

    tp.flush();
  }
}
