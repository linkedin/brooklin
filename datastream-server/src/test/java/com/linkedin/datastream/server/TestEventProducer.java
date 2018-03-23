package com.linkedin.datastream.server;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.NoOpCheckpointProvider;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


public class TestEventProducer {

  @BeforeMethod
  public void setUp() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry(), "TestEventProducer");
  }

  @AfterMethod
  public void tearDown() throws Exception {
    // A hack to force clean up DynamicMetricsManager
    Field field = DynamicMetricsManager.class.getDeclaredField("_instance");
    try {
      field.setAccessible(true);
      field.set(null, null);
    } finally {
      field.setAccessible(false);
    }
  }

  @Test
  public void testSendBasic() {
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "test-ds")[0];
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    AtomicInteger numEventsProduced = new AtomicInteger();
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        numEventsProduced.incrementAndGet();
        super.send(destination, record, onComplete);
      }
    };

    EventProducer eventProducer = new EventProducer(task, transport,
        new NoOpCheckpointProvider(), new Properties(), false);

    int eventCount = 5;
    for (int i = 0; i < eventCount; i++) {
      eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });
    }
    Assert.assertEquals(eventCount, numEventsProduced.get());
  }




  private DatastreamProducerRecord createDatastreamProducerRecord() {
    return createDatastreamProducerRecord(0, "0", 1);
  }

  private DatastreamProducerRecord createDatastreamProducerRecord(int partition, String checkpoint, int eventCount) {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.setPartition(partition);
    builder.setSourceCheckpoint(checkpoint);
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    for (int i = 0; i < eventCount; i++) {
      builder.addEvent(new BrooklinEnvelope(new byte[0], new byte[0], null, new HashMap<>()));
    }
    return builder.build();
  }
}
