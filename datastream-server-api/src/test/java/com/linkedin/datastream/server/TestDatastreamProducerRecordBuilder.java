package com.linkedin.datastream.server;

import java.io.IOException;
import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.BrooklinEnvelope;


public class TestDatastreamProducerRecordBuilder {

  @Test
  public void testBuilderWithAddDatastreamEventAndValidateFields() throws IOException {
    String sourceCheckpoint = "checkpoint";
    int partition = 0;
    long timestamp = System.currentTimeMillis();

    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    BrooklinEnvelope event1 = createDatastreamEvent();
    BrooklinEnvelope event2 = createDatastreamEvent();
    builder.addEvent(event1);
    builder.addEvent(event2);
    builder.setPartition(partition);
    builder.setEventsSourceTimestamp(timestamp);
    builder.setSourceCheckpoint(sourceCheckpoint);

    DatastreamProducerRecord record = builder.build();
    Assert.assertEquals(record.getEvents().size(), 2);
    Assert.assertEquals(record.getEvents().get(0).value().get(), event1.value().get());
    Assert.assertEquals(record.getEvents().get(1).value().get(), event2.value().get());
    Assert.assertEquals(record.getPartition().get().intValue(), partition);
    Assert.assertEquals(record.getCheckpoint(), sourceCheckpoint);
    Assert.assertEquals(record.getEventsSourceTimestamp(), timestamp);
  }

  private BrooklinEnvelope createDatastreamEvent() {
    return new BrooklinEnvelope(new byte[0], new byte[0], null, new HashMap<>());
  }

  @Test
  public void testWithoutPartitionWithoutEventsWithoutSourceCheckpoint() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    builder.setPartitionKey("foo");
    DatastreamProducerRecord record = builder.build();
    Assert.assertFalse(record.getPartition().isPresent());
    Assert.assertEquals(record.getCheckpoint(), "");
    Assert.assertNotNull(record.getEvents());
    Assert.assertEquals(record.getEvents().size(), 0);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuilderThrowsWhenPartitionIsNegative() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.setPartition(-1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testBuilderThrowsWhenEventsTimestampMissing() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();

    BrooklinEnvelope event = createDatastreamEvent();
    builder.addEvent(event);

    builder.build();
  }

  @Test
  public void testBuilderWithAddSerializedKeyValueEventAndValidateFields() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    byte[] key = "key".getBytes();
    byte[] payload = "payload".getBytes();
    builder.addEvent(key, payload, null, new HashMap<>());
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    builder.setPartition(0);
    DatastreamProducerRecord record = builder.build();

    Assert.assertEquals(record.getEvents().size(), 1);
    Assert.assertEquals(record.getEvents().get(0).key().get(), key);
    Assert.assertEquals(record.getEvents().get(0).value().get(), payload);
  }
}
