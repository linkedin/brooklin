package com.linkedin.brooklin.samza;

import org.testng.annotations.Test;

import junit.framework.Assert;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;


public class TestSamzaTransportProvider {

  @Test
  public void testSamzaTransportProviderSend() {
    SamzaTransportProviderAdmin admin = TestSamzaTransportProviderAdmin.createSamzaTransportProviderAdmin();
    DatastreamTask task = TestSamzaTransportProviderAdmin.createDatastreamTask();
    SamzaTransportProvider tp = (SamzaTransportProvider) admin.assignTransportProvider(task);
    MockSystemFactory.MockSystemProducer sp = (MockSystemFactory.MockSystemProducer) tp.getSystemProducer();
    DatastreamProducerRecord record = createRecord();
    try {
      tp.send("wrong" + TestSamzaTransportProviderAdmin.DESTINATION, record, null);
      Assert.fail();
    } catch (DatastreamRuntimeException e) {
    }

    tp.send(TestSamzaTransportProviderAdmin.DESTINATION, record, null);
    Assert.assertEquals(sp.getSentEnvelopes().size(), 1);
  }

  private DatastreamProducerRecord createRecord() {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent("key", "value");
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    builder.setPartition(0);
    builder.setSourceCheckpoint("sourceCheckpoint");
    return builder.build();
  }

  @Test
  public void testSamzaTransportProviderFlush() {

    SamzaTransportProviderAdmin admin = TestSamzaTransportProviderAdmin.createSamzaTransportProviderAdmin();
    DatastreamTask task = TestSamzaTransportProviderAdmin.createDatastreamTask();
    SamzaTransportProvider tp = (SamzaTransportProvider) admin.assignTransportProvider(task);
    MockSystemFactory.MockSystemProducer sp = (MockSystemFactory.MockSystemProducer) tp.getSystemProducer();
    DatastreamProducerRecord record = createRecord();
    tp.send(TestSamzaTransportProviderAdmin.DESTINATION, record, null);
    tp.flush();
    Assert.assertEquals(sp.getSentEnvelopes().size(), 0);
    Assert.assertEquals(sp.getFlushedEnvelopes().size(), 1);
  }
}
