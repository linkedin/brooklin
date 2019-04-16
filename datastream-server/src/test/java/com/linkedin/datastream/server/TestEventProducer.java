/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
import com.linkedin.datastream.serde.SerDe;
import com.linkedin.datastream.serde.SerDeSet;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.providers.NoOpCheckpointProvider;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


/**
 * Tests for {@link EventProducer}
 */
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

    String someTopicName = "someTopicName";
    AtomicInteger numEventsProduced = new AtomicInteger();
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        numEventsProduced.incrementAndGet();
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(null));
        onComplete.onCompletion(metadata, null);
      }
    };

    EventProducer eventProducer = new EventProducer(task, transport,
        new NoOpCheckpointProvider(), new Properties(), false);

    int eventCount = 5;
    for (int i = 0; i < eventCount; i++) {
      eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });
    }
    Assert.assertEquals(eventCount, numEventsProduced.get());

    // Verify per-topic metrics exist, since they are enabled by default
    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING));
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_SEND_LATENCY_MS_STRING));
  }



  @Test
  public void testSendWithSerdeErrors() {
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "test-ds")[0];
    datastream.getMetadata().put(EventProducer.CFG_SKIP_MSG_SERIALIZATION_ERRORS, "true");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    SerDeSet serDeSet = new SerDeSet(null, null, new SerDe() {
      @Override
      public Object deserialize(byte[] data) {
        return null;
      }

      @Override
      public byte[] serialize(Object object) {
        throw new RuntimeException();
      }
    });
    task.assignSerDes(serDeSet);



    String someTopicName = "someTopicName";
    AtomicInteger numEventsProduced = new AtomicInteger();
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        numEventsProduced.incrementAndGet();
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(null));
        onComplete.onCompletion(metadata, null);
      }
    };

    EventProducer eventProducer = new EventProducer(task, transport,
        new NoOpCheckpointProvider(), new Properties(), false);

    int eventCount = 5;
    for (int i = 0; i < eventCount; i++) {
      eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });
    }
    Assert.assertEquals(0, numEventsProduced.get());

  }

  @Test
  public void testPerDatastreamMetrics() {
    String datastreamName = "datastream-testPerDatastreamMetrics";
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider();

    Properties props = new Properties();
    props.put(EventProducer.CONFIG_ENABLE_PER_TOPIC_METRICS, Boolean.FALSE.toString());
    EventProducer eventProducer = new EventProducer(task, transport, new NoOpCheckpointProvider(), props, false);

    eventProducer.send(createDatastreamProducerRecord(), (m, e) -> {
    });

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    // Verify per-datastream metrics exist
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + datastreamName + "." + EventProducer.EVENTS_LATENCY_MS_STRING));
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + datastreamName + "." + EventProducer.EVENTS_SEND_LATENCY_MS_STRING));
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
