package com.linkedin.datastream.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.AvroUtils;
import com.linkedin.datastream.common.DatastreamEvent;


public class TestDatastreamProducerRecordBuilder {

  @Test
  public void testBuilderWithAddDatastreamEventAndValidateFields()
      throws IOException {
    String sourceCheckpoint = "checkpoint";
    int partition = 0;
    long timestamp = System.currentTimeMillis();

    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    DatastreamEvent event1 = createDatastreamEvent();
    DatastreamEvent event2 = createDatastreamEvent();
    builder.addEvent(event1);
    builder.addEvent(event2);
    builder.setPartition(partition);
    builder.setEventsTimestamp(timestamp);
    builder.setSourceCheckpoint(sourceCheckpoint);

    DatastreamProducerRecord record = builder.build();
    Assert.assertEquals(record.getEvents().size(), 2);
    Assert.assertEquals(record.getEvents().get(0).getValue(),
        AvroUtils.encodeAvroSpecificRecord(DatastreamEvent.class, event1));
    Assert.assertEquals(record.getEvents().get(1).getValue(),
        AvroUtils.encodeAvroSpecificRecord(DatastreamEvent.class, event2));
    Assert.assertEquals(record.getPartition().get().intValue(), partition);
    Assert.assertEquals(record.getCheckpoint(), sourceCheckpoint);
    Assert.assertEquals(record.getEventsTimestamp(), timestamp);
  }

  private DatastreamEvent createDatastreamEvent() {
    DatastreamEvent event = new DatastreamEvent();
    event.metadata = new HashMap<>();
    event.key = ByteBuffer.allocate(0);
    event.payload = ByteBuffer.allocate(0);
    event.previous_payload = ByteBuffer.allocate(0);
    return event;
  }

  @Test
  public void testWithoutPartitionWithoutEventsWithoutSourceCheckpoint() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.setEventsTimestamp(System.currentTimeMillis());

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

    DatastreamEvent event = createDatastreamEvent();
    builder.addEvent(event);

    builder.build();
  }

  @Test
  public void testBuilderWithAddSerializedKeyValueEventAndValidateFields() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    byte[] key = "key".getBytes();
    byte[] payload = "payload".getBytes();
    builder.addEvent(key, payload);
    builder.setEventsTimestamp(System.currentTimeMillis());
    DatastreamProducerRecord record = builder.build();

    Assert.assertEquals(record.getEvents().size(), 1);
    Assert.assertEquals(record.getEvents().get(0).getKey(), key);
    Assert.assertEquals(record.getEvents().get(0).getValue(), payload);
  }
}
