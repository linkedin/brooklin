/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors;

import java.lang.reflect.Method;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.metrics.DynamicMetricsManager;


public class TestCommonConnectorMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(TestCommonConnectorMetrics.class);
  private static final String CONSUMER1_NAME = "CONNECTOR_CONSUMER1";
  private static final String DELIMITED_CONSUMER1_NAME = "." + CONSUMER1_NAME + ".";
  private static final String CONSUMER2_NAME = "CONNECTOR_CONSUMER2";
  private static final String DELIMITED_CONSUMER2_NAME = "." + CONSUMER2_NAME + ".";
  private static final String CONSUMER3_NAME = "CONNECTOR_CONSUMER3";
  private static final String DELIMITED_AGGREGATE = "." + CommonConnectorMetrics.AGGREGATE + ".";
  private static final String CLASS_NAME = TestCommonConnectorMetrics.class.getName();

  private DynamicMetricsManager _metricsManager;

  @BeforeMethod
  public void setup(Method method) {
    _metricsManager = DynamicMetricsManager.createInstance(new MetricRegistry(), method.getName());
  }

  @Test
  public void testConnectorEventProcessingMetrics() {
    CommonConnectorMetrics connectorConsumer1 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER1_NAME, LOG);
    CommonConnectorMetrics connectorConsumer2 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER2_NAME, LOG);

    connectorConsumer1.createEventProcessingMetrics();
    connectorConsumer2.createEventProcessingMetrics();

    for (int i = 0; i < 5; i++) {
      connectorConsumer1.updateEventsProcessedRate(2);
      connectorConsumer2.updateEventsProcessedRate(3);
      connectorConsumer1.updateBytesProcessedRate(100);
      connectorConsumer2.updateBytesProcessedRate(100);
      connectorConsumer1.updateErrorRate(1);
      connectorConsumer2.updateErrorRate(2);
      connectorConsumer1.updateProcessingAboveThreshold(3);
      connectorConsumer2.updateProcessingAboveThreshold(5);
    }

    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + CommonConnectorMetrics.EventProcMetrics.EVENTS_PROCESSED_RATE)).getCount(), 10);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + CommonConnectorMetrics.EventProcMetrics.EVENTS_PROCESSED_RATE)).getCount(), 15);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
            + CommonConnectorMetrics.EventProcMetrics.EVENTS_PROCESSED_RATE)).getCount(),
        25);

    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + CommonConnectorMetrics.EventProcMetrics.EVENTS_BYTE_PROCESSED_RATE)).getCount(), 500);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + CommonConnectorMetrics.EventProcMetrics.EVENTS_BYTE_PROCESSED_RATE)).getCount(), 500);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.EventProcMetrics.EVENTS_BYTE_PROCESSED_RATE)).getCount(), 1000);

    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + CommonConnectorMetrics.EventProcMetrics.ERROR_RATE)).getCount(), 5);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + CommonConnectorMetrics.EventProcMetrics.ERROR_RATE)).getCount(), 10);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.EventProcMetrics.ERROR_RATE)).getCount(), 15);

    Assert.assertEquals(((Meter) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.EventProcMetrics.NUM_PROCESSING_ABOVE_THRESHOLD))
        .getCount(), 15);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.EventProcMetrics.NUM_PROCESSING_ABOVE_THRESHOLD))
        .getCount(), 25);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.EventProcMetrics.NUM_PROCESSING_ABOVE_THRESHOLD)).getCount(), 40);

    Instant now = Instant.now();
    connectorConsumer1.updateLastEventReceivedTime(now);
    connectorConsumer2.updateLastEventReceivedTime(now);

    int sleepTimeMS = 1000;
    try {
      Thread.sleep(sleepTimeMS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted while sleeping. Exiting test testCommonConnectorMetrics");
      return;
    }

    Assert.assertTrue((long) ((Gauge) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.EventProcMetrics.TIME_SINCE_LAST_EVENT_RECEIVED))
        .getValue() >= sleepTimeMS);
    Assert.assertTrue((long) ((Gauge) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.EventProcMetrics.TIME_SINCE_LAST_EVENT_RECEIVED))
        .getValue() >= sleepTimeMS);
  }

  @Test
  public void testConnectorPollMetrics() {
    CommonConnectorMetrics connectorConsumer1 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER1_NAME, LOG);
    CommonConnectorMetrics connectorConsumer2 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER2_NAME, LOG);

    connectorConsumer1.createPollMetrics();
    connectorConsumer2.createPollMetrics();

    for (int i = 0; i < 10; i++) {
      connectorConsumer1.updateNumPolls(1);
      connectorConsumer2.updateNumPolls(5);
      connectorConsumer1.updateEventCountsPerPoll(1 * i);
      connectorConsumer2.updateEventCountsPerPoll(5 * i);
      connectorConsumer1.updateClientPollOverTimeout(1);
      connectorConsumer2.updateClientPollOverTimeout(2);
      connectorConsumer1.updateClientPollIntervalOverSessionTimeout(3);
      connectorConsumer2.updateClientPollIntervalOverSessionTimeout(4);
    }
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + CommonConnectorMetrics.PollMetrics.NUM_POLLS)).getCount(), 10);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + CommonConnectorMetrics.PollMetrics.NUM_POLLS)).getCount(), 50);
    Assert.assertEquals(((Histogram) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + CommonConnectorMetrics.PollMetrics.EVENT_COUNTS_PER_POLL)).getSnapshot()
        .get99thPercentile(), 9.0);
    Assert.assertEquals(((Histogram) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + CommonConnectorMetrics.PollMetrics.EVENT_COUNTS_PER_POLL)).getSnapshot()
        .get99thPercentile(), 45.0);
    Assert.assertEquals(((Counter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
            + CommonConnectorMetrics.PollMetrics.CLIENT_POLL_OVER_TIMEOUT)).getCount(),
        30);
    Assert.assertEquals(((Counter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PollMetrics.CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT)).getCount(), 70);
  }

  @Test
  public void testPConnectorPartitionMetrics() {
    CommonConnectorMetrics connectorConsumer1 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER1_NAME, LOG);
    CommonConnectorMetrics connectorConsumer2 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER2_NAME, LOG);

    connectorConsumer1.createPartitionMetrics();
    connectorConsumer2.createPartitionMetrics();

    for (int i = 0; i < 5; i++) {
      connectorConsumer1.updateRebalanceRate(1);
      connectorConsumer2.updateRebalanceRate(2);
    }
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + CommonConnectorMetrics.PartitionMetrics.REBALANCE_RATE)).getCount(), 5);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
            + CommonConnectorMetrics.PartitionMetrics.REBALANCE_RATE)).getCount(),
        10);
    Assert.assertEquals(((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.REBALANCE_RATE)).getCount(), 15);

    connectorConsumer1.updateStuckPartitions(10);
    connectorConsumer2.updateStuckPartitions(20);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        10);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        20);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(), 30);
    connectorConsumer1.updateStuckPartitions(5);
    connectorConsumer2.updateStuckPartitions(12);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        5);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        12);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(), 17);

    connectorConsumer1.updateStuckPartitions(0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        12);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(), 12);

    connectorConsumer2.resetStuckPartitions();
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
            + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(),
        0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(), 0);
  }

  @Test
  public void testDeregisterMetrics() {
    CommonConnectorMetrics metrics1 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER1_NAME, LOG);
    CommonConnectorMetrics metrics2 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER2_NAME, LOG);

    metrics1.createPartitionMetrics();
    metrics2.createPartitionMetrics();

    metrics1.updateNumPartitions(15);
    metrics2.updateNumPartitions(10);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.NUM_PARTITIONS)).getValue(), 25);

    metrics1.updateStuckPartitions(10);
    metrics2.updateStuckPartitions(15);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(), 25);

    Assert.assertNotNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, "aggregate", "stuckPartitions")));
    Assert.assertNotNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, CONSUMER1_NAME, "stuckPartitions")));
    Assert.assertNotNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, CONSUMER2_NAME, "stuckPartitions")));

    metrics1.deregisterMetrics();
    Assert.assertNotNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, "aggregate", "stuckPartitions")));
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, CONSUMER1_NAME, "stuckPartitions")));

    // Aggregate metrics' value should be only reflect value of valid registered metrics. Any metrics that have been
    // deregistered should be subtracted from aggregate metrics.
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.NUM_PARTITIONS)).getValue(), 10);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE
        + CommonConnectorMetrics.PartitionMetrics.STUCK_PARTITIONS)).getValue(), 15);

    metrics2.deregisterMetrics();
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, "aggregate", "stuckPartitions")));
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, CONSUMER2_NAME, "stuckPartitions")));

    CommonConnectorMetrics metrics3 = new CommonConnectorMetrics(CLASS_NAME, CONSUMER3_NAME, LOG);
    metrics3.createPartitionMetrics();

    Assert.assertNotNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, "aggregate", "stuckPartitions")));
    Assert.assertNotNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, CONSUMER3_NAME, "stuckPartitions")));

    metrics3.deregisterMetrics();
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, "aggregate", "stuckPartitions")));
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, CONSUMER3_NAME, "stuckPartitions")));
  }
}