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

import com.codahale.metrics.Meter;
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
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };

    EventProducer eventProducer =
        new EventProducer(task, transport, new NoOpCheckpointProvider(), new Properties(), false);

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
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };

    EventProducer eventProducer =
        new EventProducer(task, transport, new NoOpCheckpointProvider(), new Properties(), false);

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

  @Test
  public void testThroughputAttributionMetrics() {
    String datastreamName = "datastream-testThroughputAttributionMetrics";
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    byte[] key = new byte[10];
    byte[] value = new byte[20];
    long expectedBytes = key.length + value.length; // 30

    String someTopicName = "someTopicName";
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };

    Properties props = new Properties();
    props.put(EventProducer.CONFIG_ENABLE_THROUGHPUT_METRICS, Boolean.TRUE.toString());
    EventProducer eventProducer =
        new EventProducer(task, transport, new NoOpCheckpointProvider(), props, false);

    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.setPartition(0);
    builder.setSourceCheckpoint("0");
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    builder.addEvent(new BrooklinEnvelope(key, value, null, new HashMap<>()));
    eventProducer.send(builder.build(), (m, e) -> { });

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    String connectorType = DummyConnector.CONNECTOR_TYPE;

    Meter dbBytesRate = (Meter) metrics.getMetric("EventProducer.db.testDatabase." + EventProducer.BYTES_PRODUCED_RATE);
    Assert.assertNotNull(dbBytesRate, "Per-database bytesProducedRate should exist");
    Assert.assertEquals(dbBytesRate.getCount(), expectedBytes);

    Meter dbEventRate = (Meter) metrics.getMetric("EventProducer.db.testDatabase.eventProduceRate");
    Assert.assertNotNull(dbEventRate, "Per-database eventProduceRate should exist");
    Assert.assertEquals(dbEventRate.getCount(), 1);

    Meter aggBytesRate = (Meter) metrics.getMetric("EventProducer.aggregate." + EventProducer.BYTES_PRODUCED_RATE);
    Assert.assertNotNull(aggBytesRate, "Aggregate bytesProducedRate should exist");
    Assert.assertEquals(aggBytesRate.getCount(), expectedBytes);

    Meter connectorBytesRate = (Meter) metrics.getMetric("EventProducer." + connectorType + "." + EventProducer.BYTES_PRODUCED_RATE);
    Assert.assertNotNull(connectorBytesRate, "Connector-type bytesProducedRate should exist");
    Assert.assertEquals(connectorBytesRate.getCount(), expectedBytes);
  }

  @Test
  public void testThroughputMetricsDisabledByDefault() {
    String datastreamName = "datastream-testThroughputMetricsDisabled";
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    datastream.getSource().setConnectionString("mysql:/myhost/someDatabase/someTable");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    String someTopicName = "someTopicName";
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };

    // Do NOT set CONFIG_ENABLE_THROUGHPUT_METRICS — it should default to false
    EventProducer eventProducer =
        new EventProducer(task, transport, new NoOpCheckpointProvider(), new Properties(), false);

    eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(metrics.getMetric("EventProducer.db.someDatabase." + EventProducer.BYTES_PRODUCED_RATE),
        "bytesProducedRate should not exist when throughput metrics are disabled");
    Assert.assertNull(metrics.getMetric("EventProducer.aggregate." + EventProducer.BYTES_PRODUCED_RATE),
        "aggregate bytesProducedRate should not exist when throughput metrics are disabled");
  }

  @Test
  public void testNoDatabaseMetricForBmmUri() {
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "datastream-testBmmUri")[0];
    // BMM source uses double-slash URI — no database segment should be extracted
    datastream.getSource().setConnectionString("kafka://broker:9092/someTopic");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    Properties props = new Properties();
    props.put(EventProducer.CONFIG_ENABLE_THROUGHPUT_METRICS, Boolean.TRUE.toString());
    EventProducer eventProducer =
        new EventProducer(task, new NoOpTransportProviderAdminFactory.NoOpTransportProvider(),
            new NoOpCheckpointProvider(), props, false);

    eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(metrics.getMetric("EventProducer.db.someTopic." + EventProducer.BYTES_PRODUCED_RATE),
        "No per-database metric should exist for BMM double-slash URI");
    Assert.assertNotNull(metrics.getMetric("EventProducer.aggregate." + EventProducer.BYTES_PRODUCED_RATE),
        "Aggregate bytesProducedRate should still exist for BMM");
  }

  @Test
  public void testDisableSlaMetricSuppressesEventsLatencyMs() {
    String datastreamName = "datastream-testDisableSlaMetric";
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    datastream.getMetadata().put(EventProducer.CFG_DISABLE_SLA_METRIC, "true");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    String someTopicName = "someTopicName";
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };

    EventProducer eventProducer =
        new EventProducer(task, transport, new NoOpCheckpointProvider(), new Properties(), false);

    eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    String connectorType = DummyConnector.CONNECTOR_TYPE;

    // eventsLatencyMs must NOT be emitted at any level when flag is set
    Assert.assertNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "Per-topic eventsLatencyMs should not exist when system.disableSlaMetric=true");
    Assert.assertNull(
        metrics.getMetric("EventProducer.aggregate." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "Aggregate eventsLatencyMs should not exist when system.disableSlaMetric=true");
    Assert.assertNull(
        metrics.getMetric("EventProducer." + connectorType + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "Connector-type eventsLatencyMs should not exist when system.disableSlaMetric=true");

    // Within/outside SLA counters must still be emitted
    Assert.assertNotNull(metrics.getMetric("EventProducer.aggregate.eventsProducedWithinSla"),
        "Aggregate eventsProducedWithinSla should still exist when SLA metric is disabled");
    Assert.assertNotNull(metrics.getMetric("EventProducer.aggregate.eventsProducedOutsideSla"),
        "Aggregate eventsProducedOutsideSla should still exist when SLA metric is disabled");

    // totalEventsProduced and eventProduceRate must still be emitted
    Assert.assertNotNull(metrics.getMetric("EventProducer.aggregate.totalEventsProduced"),
        "totalEventsProduced should still exist when SLA metric is disabled");
    Assert.assertNotNull(metrics.getMetric("EventProducer.aggregate.eventProduceRate"),
        "eventProduceRate should still exist when SLA metric is disabled");

    // eventsSendLatencyMs is unrelated to the flag and must still be emitted
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_SEND_LATENCY_MS_STRING),
        "eventsSendLatencyMs should still exist when SLA metric is disabled");
  }

  @Test
  public void testDisableSlaMetricDefaultEmitsEventsLatencyMs() {
    String datastreamName = "datastream-testDisableSlaMetricDefault";
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    // Do NOT set CFG_DISABLE_SLA_METRIC — should default to false
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    String someTopicName = "someTopicName";
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };

    EventProducer eventProducer =
        new EventProducer(task, transport, new NoOpCheckpointProvider(), new Properties(), false);

    eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "eventsLatencyMs should be emitted by default (flag absent)");
  }

  @Test
  public void testDisableSlaMetricDoesNotAffectThroughputViolatingPath() {
    String datastreamName = "datastream-testDisableSlaMetricThroughput";
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    datastream.getMetadata().put(EventProducer.CFG_DISABLE_SLA_METRIC, "true");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));

    String someTopicName = "throughputViolatingTopic";
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), someTopicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };

    // Provider routes this topic to the throughput-violating path
    EventProducer eventProducer = new EventProducer(task, transport, new NoOpCheckpointProvider(),
        new Properties(), false, t -> Collections.singleton(someTopicName));

    eventProducer.send(createDatastreamProducerRecord(), (m, e) -> { });

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();

    // The throughput-violating latency metric must keep flowing even when the SLA flag is set
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + someTopicName + "."
            + EventProducer.THROUGHPUT_VIOLATING_EVENTS_LATENCY_MS_STRING),
        "throughputViolatingEventsLatencyMs should still be emitted when system.disableSlaMetric=true");
    Assert.assertNotNull(
        metrics.getMetric("EventProducer.aggregate." + EventProducer.THROUGHPUT_VIOLATING_EVENTS_LATENCY_MS_STRING),
        "Aggregate throughputViolatingEventsLatencyMs should still be emitted");

    // The standard eventsLatencyMs is on the other branch and shouldn't be emitted at all here
    Assert.assertNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "Standard eventsLatencyMs should not be emitted on the throughput-violating path");
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
