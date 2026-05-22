/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
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

  // ---------------------------------------------------------------------------
  // SLA grace-period tests
  //
  // Aggregate-level counters are used as the assertion surface:
  //   - EventProducer.aggregate.eventsProducedOutsideSla is pre-registered in the
  //     constructor (count 0), so it always exists.
  //   - EventProducer.aggregate.eventsProducedWithinSla is NOT pre-registered.
  //     Its presence/absence is therefore a clean signal of whether
  //     reportSLAMetrics() ran (i.e. whether the grace gate let the call through).
  // ---------------------------------------------------------------------------

  private static final String SLA_WITHIN_AGG = "EventProducer.aggregate.eventsProducedWithinSla";
  private static final String SLA_WITHIN_ALT_AGG = "EventProducer.aggregate.eventsProducedWithinAlternateSla";
  private static final String COMMIT_WITHIN_AGG = "EventProducer.aggregate.eventsCommitWithinSla";
  private static final String COMMIT_OUTSIDE_AGG = "EventProducer.aggregate.eventsCommitOutsideSla";

  // For commit-to-ack metric assertions, use a non-CDC source (kafka://) so the grace gate is not
  // engaged — the new metric should fire whenever the connector supplies a commit timestamp,
  // regardless of CDC catch-up logic.
  private static String setupOldNonCdcStream(Datastream datastream) {
    datastream.getSource().setConnectionString("kafka://broker:9092/topic");
    long oneHourAgo = System.currentTimeMillis() - (60 * 60 * 1000L);
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(oneHourAgo));
    return datastream.getName();
  }

  @Test
  public void testCommitToAckMetricNotEmittedWhenCommitTimestampAbsent() {
    // No commit timestamp on the record → new metric path is a no-op; existing metrics are unaffected.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-no-commit-ts")[0];
    setupOldNonCdcStream(datastream);

    String topic = "noCommitTsTopic";
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    sendOneEventThroughTask(task, new Properties(), topic, null);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(
        metrics.getMetric("EventProducer." + topic + "." + EventProducer.EVENTS_COMMIT_TO_ACK_LATENCY_MS_STRING),
        "commit-to-ack histogram must not fire when the record has no commit timestamp");
    Assert.assertNull(metrics.getMetric(COMMIT_WITHIN_AGG),
        "commit-to-ack within-SLA counter must not be created when no commit timestamp is supplied");
    Assert.assertNull(metrics.getMetric(COMMIT_OUTSIDE_AGG),
        "commit-to-ack outside-SLA counter must not be created when no commit timestamp is supplied");
    Assert.assertNotNull(metrics.getMetric(SLA_WITHIN_AGG),
        "existing eventsLatencyMs SLA path must still fire — no regression");
  }

  @Test
  public void testCommitToAckMetricFiresWithinSlaWhenCommitTimestampRecent() {
    // Recent commit timestamp → within default 5-min SLA → withinSla counter increments, histogram emits.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-commit-within")[0];
    setupOldNonCdcStream(datastream);

    String topic = "commitWithinSlaTopic";
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    sendOneEventThroughTask(task, new Properties(), topic, System.currentTimeMillis());

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + topic + "." + EventProducer.EVENTS_COMMIT_TO_ACK_LATENCY_MS_STRING),
        "commit-to-ack histogram must fire when commit timestamp is present");
    Counter withinAgg = (Counter) metrics.getMetric(COMMIT_WITHIN_AGG);
    Assert.assertNotNull(withinAgg, "withinSla aggregate counter must be created when commit timestamp is present");
    Assert.assertEquals(withinAgg.getCount(), 1L, "recent commit timestamp should fall inside the default 5-min SLA");
  }

  @Test
  public void testCommitToAckMetricFiresOutsideSlaWhenThresholdTight() {
    // Force OUTSIDE_SLA by setting a 1ms threshold; any latency by the time the callback runs blows past it.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-commit-outside")[0];
    setupOldNonCdcStream(datastream);

    Properties props = new Properties();
    props.put("commitToAckThresholdSlaMs", "1");

    String topic = "commitOutsideSlaTopic";
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    sendOneEventThroughTask(task, props, topic, System.currentTimeMillis() - 100);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Counter outsideAgg = (Counter) metrics.getMetric(COMMIT_OUTSIDE_AGG);
    Assert.assertNotNull(outsideAgg, "outsideSla counter must be created when commit-to-ack latency exceeds threshold");
    Assert.assertEquals(outsideAgg.getCount(), 1L,
        "100ms commit-to-ack latency vs 1ms threshold should count as outside-SLA");
  }

  @Test
  public void testCommitToAckMetricRedirectedToSlaIneligibleDuringGracePeriod() {
    // CDC+BST source + freshly created stream → grace gate engaged. Commit-to-ack histogram should redirect
    // to the SLA-ineligible variant and both within/outside counters should remain suppressed —
    // same suppression semantics as the existing eventsLatencyMs path.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-commit-grace")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS,
        String.valueOf(System.currentTimeMillis()));
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    Properties props = new Properties();
    props.put("newStreamGracePeriodMs", "7200000");

    String topic = "commitGraceTopic";
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    sendOneEventThroughTask(task, props, topic, System.currentTimeMillis());

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(
        metrics.getMetric("EventProducer." + topic + "." + EventProducer.EVENTS_COMMIT_TO_ACK_LATENCY_MS_STRING),
        "commit-to-ack histogram must NOT fire during grace period");
    Assert.assertNotNull(
        metrics.getMetric(
            "EventProducer." + topic + "." + EventProducer.EVENTS_COMMIT_TO_ACK_LATENCY_MS_SLA_INELIGIBLE_STRING),
        "commit-to-ack latency should be redirected to the SLA-ineligible histogram during grace");
    Assert.assertNull(metrics.getMetric(COMMIT_WITHIN_AGG),
        "commit-to-ack withinSla counter must remain suppressed during grace");
    Assert.assertNull(metrics.getMetric(COMMIT_OUTSIDE_AGG),
        "commit-to-ack outsideSla counter must remain suppressed during grace");
  }

  @Test
  public void testSlaGraceActiveForNewCdcBstStream() {
    // CDC+BST source (single-slash mysql:/ + cdcBootstrapRequired=true) + freshly-created stream
    // → grace gate engaged. Both primary and alternate SLA counter pairs are suppressed entirely.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-bst-new")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS,
        String.valueOf(System.currentTimeMillis()));
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");
    Properties props = new Properties();
    props.put("newStreamGracePeriodMs", "7200000"); // explicit 2h grace period
    sendOneEventThroughProducer(datastream, props);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_AGG),
        "Primary withinSla counter must not be created during grace period for new CDC+BST stream");
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_ALT_AGG),
        "Alternate-SLA counter must not be created during grace period for new CDC+BST stream");
  }

  @Test
  public void testSlaGraceNotAppliedToCdcOnlyStream() {
    // Pure CDC-only source (no cdcBootstrapRequired flag) → isCdcSource() returns false →
    // grace gate never engaged, SLA is reported from the very first event regardless of stream age.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-only")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS,
        String.valueOf(System.currentTimeMillis()));
    // Intentionally NOT setting cdcBootstrapRequired — this is a pure CDC-only stream
    Properties props = new Properties();
    props.put("newStreamGracePeriodMs", "7200000");
    sendOneEventThroughProducer(datastream, props);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Counter withinAgg = (Counter) metrics.getMetric(SLA_WITHIN_AGG);
    Assert.assertNotNull(withinAgg,
        "Pure CDC-only streams must report SLA immediately — grace suppression only applies to CDC+BST");
    Assert.assertEquals(withinAgg.getCount(), 1L);
  }

  @Test
  public void testSlaGraceNotAppliedToCdcOnlyStreamWithFalseFlag() {
    // CDC source with cdcBootstrapRequired=false → same as no flag; SLA always reported.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-false")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS,
        String.valueOf(System.currentTimeMillis()));
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "false");
    Properties props = new Properties();
    props.put("newStreamGracePeriodMs", "7200000");
    sendOneEventThroughProducer(datastream, props);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Counter withinAgg = (Counter) metrics.getMetric(SLA_WITHIN_AGG);
    Assert.assertNotNull(withinAgg,
        "cdcBootstrapRequired=false must not suppress SLA — flag must be explicitly true");
    Assert.assertEquals(withinAgg.getCount(), 1L);
  }

  @Test
  public void testSlaGraceExpiredForOldCdcBstStream() {
    // CDC+BST source + creation timestamp older than the 2h grace window → SLA reporting active.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-bst-old")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    long threeHoursAgo = System.currentTimeMillis() - (3 * 60 * 60 * 1000L);
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(threeHoursAgo));
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");
    sendOneEventThroughProducer(datastream, new Properties());

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Counter withinAgg = (Counter) metrics.getMetric(SLA_WITHIN_AGG);
    Assert.assertNotNull(withinAgg, "withinSla counter must be created once CDC+BST grace period has expired");
    Assert.assertEquals(withinAgg.getCount(), 1L,
        "Single send past grace window should be reported as within SLA");
  }

  @Test
  public void testSlaGraceNotAppliedToMirrorMakerSource() {
    // BMM source (double-slash kafka://) → _sourceDatabase == null → grace gate disabled regardless
    // of CREATION_MS. This is the primary regression guard for the MM-exclusion fix.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-bmm-new")[0];
    datastream.getSource().setConnectionString("kafka://broker:9092/someTopic");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS,
        String.valueOf(System.currentTimeMillis()));
    sendOneEventThroughProducer(datastream, new Properties());

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Counter withinAgg = (Counter) metrics.getMetric(SLA_WITHIN_AGG);
    Assert.assertNotNull(withinAgg, "MirrorMaker streams must report SLA from the first event, regardless of stream age");
    Assert.assertEquals(withinAgg.getCount(), 1L);
  }

  @Test
  public void testSlaGraceFailsOpenWhenCreationMsMissing() {
    // CDC+BST source but no CREATION_MS metadata → _streamCreationTimeMs stays 0 → gate disabled (fail-open).
    // Note: DatastreamTestUtils.createDatastreams auto-populates CREATION_MS, so we explicitly remove
    // it to exercise the missing-metadata path.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-nomd")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().remove(DatastreamMetadataConstants.CREATION_MS);
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");
    sendOneEventThroughProducer(datastream, new Properties());

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(metrics.getMetric(SLA_WITHIN_AGG),
        "Missing CREATION_MS must fail open to SLA reporting, not silently suppress it");
  }

  @Test
  public void testSlaGraceFailsOpenWhenCreationMsMalformed() {
    // CDC+BST source with malformed CREATION_MS → NumberFormatException caught in constructor → grace disabled.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-bad")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, "not-a-long");
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");
    sendOneEventThroughProducer(datastream, new Properties());

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(metrics.getMetric(SLA_WITHIN_AGG),
        "Malformed CREATION_MS must fail open to SLA reporting");
  }

  @Test
  public void testLatencyHistogramRedirectedToSlaIneligibleDuringGracePeriod() {
    // During CDC+BST grace, the lag histogram is redirected from eventsLatencyMs to
    // eventsLatencyMsSlaIneligible so lag alerts on the primary metric do not fire on initial
    // CDC+BST catch-up. Both primary and alternate SLA counters are suppressed entirely during
    // the grace window.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-latency")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS,
        String.valueOf(System.currentTimeMillis()));
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    Properties props = new Properties();
    props.put("newStreamGracePeriodMs", "7200000"); // explicit 2h grace period
    String someTopicName = "graceLatencyTopic";
    sendOneEventThroughProducer(datastream, props, someTopicName);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "eventsLatencyMs must NOT fire during grace period — that's what lag alerts are wired to");
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_SLA_INELIGIBLE_STRING),
        "eventsLatencyMsSlaIneligible should receive the redirected latency observation during grace");
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_ALT_AGG),
        "Alternate-SLA counter should remain suppressed during grace period");
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_AGG),
        "Primary SLA counter should remain suppressed during grace period");
  }

  @Test
  public void testLatencyHistogramFiresOnPrimaryAfterGracePeriod() {
    // Post-grace CDC+BST: latency observations go back to eventsLatencyMs (the metric lag alerts watch).
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-latency-old")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    long threeHoursAgo = System.currentTimeMillis() - (3 * 60 * 60 * 1000L);
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(threeHoursAgo));
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    String someTopicName = "postGraceLatencyTopic";
    sendOneEventThroughProducer(datastream, new Properties(), someTopicName);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "eventsLatencyMs must fire once the grace period has expired");
    Assert.assertNull(
        metrics.getMetric("EventProducer." + someTopicName + "." + EventProducer.EVENTS_LATENCY_MS_SLA_INELIGIBLE_STRING),
        "eventsLatencyMsSlaIneligible should not be touched outside the grace window");
  }

  @Test
  public void testCustomGracePeriodOverride() {
    // Operator-specified grace period should win over the default. With a 1ms window, a CDC+BST stream
    // created "now" should already be past grace by the time the producer records its first event.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-cdc-custom")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS,
        String.valueOf(System.currentTimeMillis() - 10));
    datastream.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    Properties props = new Properties();
    props.put("newStreamGracePeriodMs", "1");
    sendOneEventThroughProducer(datastream, props);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(metrics.getMetric(SLA_WITHIN_AGG),
        "Custom 1ms grace period should expire before the first event is reported");
  }

  @Test
  public void testSlaGraceDedupedTaskUsesOldestCreationTime() {
    // Two CDC+BST datastreams deduped onto one task: one created 3h ago, one created now.
    // Grace gate must follow the OLDEST stream (3h ago, past grace) → SLA reporting active.
    long now = System.currentTimeMillis();
    long threeHoursAgo = now - (3 * 60 * 60 * 1000L);

    Datastream oldDs = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-old")[0];
    oldDs.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    oldDs.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(threeHoursAgo));
    oldDs.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    Datastream newDs = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-new")[0];
    newDs.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    newDs.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(now));
    newDs.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Arrays.asList(oldDs, newDs));
    sendOneEventThroughTask(task, new Properties(), "someTopicName");

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNotNull(metrics.getMetric(SLA_WITHIN_AGG),
        "Deduped task with at least one old stream must report SLA (oldest stream is past grace)");
  }

  @Test
  public void testSlaGraceDedupedTaskAllStreamsNew() {
    // All CDC+BST streams on the deduped task are within the grace window → SLA suppressed.
    long now = System.currentTimeMillis();

    Datastream newDsA = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-newA")[0];
    newDsA.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    newDsA.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(now));
    newDsA.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    Datastream newDsB = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-newB")[0];
    newDsB.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    newDsB.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(now - 60_000L));
    newDsB.getMetadata().put(DatastreamMetadataConstants.CDC_BOOTSTRAP_REQUIRED_KEY, "true");

    Properties props = new Properties();
    props.put("newStreamGracePeriodMs", "7200000"); // explicit 2h grace period
    DatastreamTaskImpl task = new DatastreamTaskImpl(Arrays.asList(newDsA, newDsB));
    sendOneEventThroughTask(task, props, "someTopicName");

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_AGG),
        "All streams within grace window → primary SLA suppressed across the deduped task");
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_ALT_AGG),
        "Alternate-SLA counter must remain suppressed during grace across the deduped task");
  }

  // ---------------------------------------------------------------------------
  // system.disableSlaMetric opt-out tests
  //
  // The opt-out is a per-datastream metadata flag that, when set, suppresses
  // primary + alternate SLA counters and redirects the latency histogram from
  // eventsLatencyMs to eventsLatencyMsSlaIneligible. Honored only for tasks
  // that own a single datastream — deduped tasks ignore the flag so one
  // stream's opt-out cannot suppress eventsLatencyMs for the whole group.
  //
  // Tests use a creation timestamp 3h in the past to bypass the CDC grace
  // gate, so the only thing being exercised is the opt-out path itself.
  // ---------------------------------------------------------------------------

  @Test
  public void testDisableSlaMetricSuppressesSlaCountersAndRedirectsLatency() {
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-opt-out")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    long threeHoursAgo = System.currentTimeMillis() - (3 * 60 * 60 * 1000L);
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(threeHoursAgo));
    datastream.getMetadata().put(EventProducer.CFG_DISABLE_SLA_METRIC, Boolean.TRUE.toString());

    String topicName = "optOutLatencyTopic";
    sendOneEventThroughProducer(datastream, new Properties(), topicName);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_AGG),
        "Primary withinSla counter must not be created when system.disableSlaMetric=true");
    Assert.assertNull(metrics.getMetric(SLA_WITHIN_ALT_AGG),
        "Alternate-SLA counter must not be created when system.disableSlaMetric=true");
    Assert.assertNull(
        metrics.getMetric("EventProducer." + topicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "eventsLatencyMs must not fire for opted-out streams");
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + topicName + "." + EventProducer.EVENTS_LATENCY_MS_SLA_INELIGIBLE_STRING),
        "Latency observation should be redirected to eventsLatencyMsSlaIneligible for opted-out streams");
  }

  @Test
  public void testDisableSlaMetricFalseLeavesSlaReportingActive() {
    // Explicit false should behave identically to the flag being absent.
    Datastream datastream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-opt-out-false")[0];
    datastream.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    long threeHoursAgo = System.currentTimeMillis() - (3 * 60 * 60 * 1000L);
    datastream.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(threeHoursAgo));
    datastream.getMetadata().put(EventProducer.CFG_DISABLE_SLA_METRIC, Boolean.FALSE.toString());

    String topicName = "optOutFalseTopic";
    sendOneEventThroughProducer(datastream, new Properties(), topicName);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Counter withinAgg = (Counter) metrics.getMetric(SLA_WITHIN_AGG);
    Assert.assertNotNull(withinAgg, "withinSla counter should be created when opt-out flag is false");
    Assert.assertEquals(withinAgg.getCount(), 1L);
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + topicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "eventsLatencyMs must fire normally when opt-out flag is false");
  }

  @Test
  public void testDisableSlaMetricIgnoredForDedupedTask() {
    // Two datastreams sharing one EventProducer: one opted out, one not. The opt-out must be
    // ignored so the non-opted-out stream's SLA reporting is preserved.
    long threeHoursAgo = System.currentTimeMillis() - (3 * 60 * 60 * 1000L);

    Datastream optedOut = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-dedup-optout")[0];
    optedOut.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    optedOut.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(threeHoursAgo));
    optedOut.getMetadata().put(EventProducer.CFG_DISABLE_SLA_METRIC, Boolean.TRUE.toString());

    Datastream regular = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, "ds-dedup-regular")[0];
    regular.getSource().setConnectionString("mysql:/myhost/testDatabase/myTable");
    regular.getMetadata().put(DatastreamMetadataConstants.CREATION_MS, String.valueOf(threeHoursAgo));

    DatastreamTaskImpl task = new DatastreamTaskImpl(Arrays.asList(optedOut, regular));
    String topicName = "dedupOptOutTopic";
    sendOneEventThroughTask(task, new Properties(), topicName);

    DynamicMetricsManager metrics = DynamicMetricsManager.getInstance();
    Counter withinAgg = (Counter) metrics.getMetric(SLA_WITHIN_AGG);
    Assert.assertNotNull(withinAgg,
        "Deduped task must ignore opt-out — one stream's flag cannot suppress SLA for the whole group");
    Assert.assertEquals(withinAgg.getCount(), 1L);
    Assert.assertNotNull(
        metrics.getMetric("EventProducer." + topicName + "." + EventProducer.EVENTS_LATENCY_MS_STRING),
        "eventsLatencyMs must continue to fire for deduped tasks regardless of any member's opt-out flag");
  }

  private void sendOneEventThroughProducer(Datastream datastream, Properties props) {
    sendOneEventThroughProducer(datastream, props, "someTopicName");
  }

  private void sendOneEventThroughProducer(Datastream datastream, Properties props, String topicName) {
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    sendOneEventThroughTask(task, props, topicName);
  }

  private void sendOneEventThroughTask(DatastreamTaskImpl task, Properties props, String topicName) {
    sendOneEventThroughTask(task, props, topicName, null);
  }

  private void sendOneEventThroughTask(DatastreamTaskImpl task, Properties props, String topicName,
      Long commitTimestamp) {
    TransportProvider transport = new NoOpTransportProviderAdminFactory.NoOpTransportProvider() {
      @Override
      public void send(String destination, DatastreamProducerRecord record, SendCallback onComplete) {
        DatastreamRecordMetadata metadata =
            new DatastreamRecordMetadata(record.getCheckpoint(), topicName, record.getPartition().orElse(0));
        onComplete.onCompletion(metadata, null);
      }
    };
    EventProducer eventProducer = new EventProducer(task, transport, new NoOpCheckpointProvider(), props, false);
    eventProducer.send(createDatastreamProducerRecord(commitTimestamp), (m, e) -> { });
  }

  private DatastreamProducerRecord createDatastreamProducerRecord() {
    return createDatastreamProducerRecord(0, "0", 1);
  }

  private DatastreamProducerRecord createDatastreamProducerRecord(Long commitTimestamp) {
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.setPartition(0);
    builder.setSourceCheckpoint("0");
    builder.setEventsSourceTimestamp(System.currentTimeMillis());
    if (commitTimestamp != null) {
      builder.setEventsCommitTimestamp(commitTimestamp);
    }
    builder.addEvent(new BrooklinEnvelope(new byte[0], new byte[0], null, new HashMap<>()));
    return builder.build();
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
