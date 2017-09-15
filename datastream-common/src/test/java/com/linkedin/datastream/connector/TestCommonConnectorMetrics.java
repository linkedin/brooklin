package com.linkedin.datastream.connector;

import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;
import com.linkedin.datastream.metrics.DynamicMetricsManager;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

public class TestCommonConnectorMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(TestCommonConnectorMetrics.class);
  private static final String CONSUMER1_NAME = "CONNECTOR_CONSUMER1";
  private static final String DELIMITED_CONSUMER1_NAME = "." + CONSUMER1_NAME + ".";
  private static final String CONSUMER2_NAME = "CONNECTOR_CONSUMER2";
  private static final String DELIMITED_CONSUMER2_NAME = "." + CONSUMER2_NAME + ".";
  private static final String DELIMITED_AGGREGATE = "." + CommonConnectorMetrics.AGGREGATE + ".";
  private static final String CLASS_NAME = TestCommonConnectorMetrics.class.getName();

  private static final DynamicMetricsManager METRICS_MANAGER = DynamicMetricsManager.createInstance(new MetricRegistry());

  private static final CommonConnectorMetrics CONNECTOR_CONSUMER1 =
      new CommonConnectorMetrics(CLASS_NAME, CONSUMER1_NAME);
  private static final CommonConnectorMetrics CONNECTOR_CONSUMER2 =
      new CommonConnectorMetrics(CLASS_NAME, CONSUMER2_NAME);

  @Test
  public void testConnectorEventProcessingMetrics() {
    CONNECTOR_CONSUMER1.createEventProcessingMetrics();
    CONNECTOR_CONSUMER2.createEventProcessingMetrics();

    for (int i = 0; i < 5; i++) {
      CONNECTOR_CONSUMER1.updateEventsProcessedRate(2);
      CONNECTOR_CONSUMER2.updateEventsProcessedRate(3);
      CONNECTOR_CONSUMER1.updateBytesProcessedRate(100);
      CONNECTOR_CONSUMER2.updateBytesProcessedRate(100);
      CONNECTOR_CONSUMER1.updateErrorRate(1);
      CONNECTOR_CONSUMER2.updateErrorRate(2);
      CONNECTOR_CONSUMER1.updateProcessingAboveThreshold(3);
      CONNECTOR_CONSUMER2.updateProcessingAboveThreshold(5);
    }

    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.EVENTS_PROCESSED_RATE)).getCount(), 10);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.EVENTS_PROCESSED_RATE)).getCount(), 15);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.EVENTS_PROCESSED_RATE)).getCount(), 25);

    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.EVENTS_BYTE_PROCESSED_RATE)).getCount(), 500);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.EVENTS_BYTE_PROCESSED_RATE)).getCount(), 500);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.EVENTS_BYTE_PROCESSED_RATE)).getCount(), 1000);

    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.ERROR_RATE)).getCount(), 5);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.ERROR_RATE)).getCount(), 10);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.ERROR_RATE)).getCount(), 15);

    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME +
            CommonConnectorMetrics.NUM_PROCESSING_ABOVE_THRESHOLD)).getCount(), 15);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER2_NAME +
            CommonConnectorMetrics.NUM_PROCESSING_ABOVE_THRESHOLD)).getCount(), 25);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(
        CLASS_NAME + DELIMITED_AGGREGATE +
            CommonConnectorMetrics.NUM_PROCESSING_ABOVE_THRESHOLD)).getCount(), 40);

    Instant now = Instant.now();
    CONNECTOR_CONSUMER1.updateLastEventReceivedTime(now);
    CONNECTOR_CONSUMER2.updateLastEventReceivedTime(now);

    int sleepTimeMS = 1000;
    try {
      Thread.sleep(sleepTimeMS);
    } catch (InterruptedException e) {
      LOG.info("Thread interrupted while sleeping. Exiting test testCommonConnectorMetrics");
      return;
    }

    Assert.assertTrue((long) ((Gauge) METRICS_MANAGER.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME +
            CommonConnectorMetrics.TIME_SINCE_LAST_EVENT_RECEIVED)).getValue() >= sleepTimeMS);
    Assert.assertTrue((long) ((Gauge) METRICS_MANAGER.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER2_NAME +
            CommonConnectorMetrics.TIME_SINCE_LAST_EVENT_RECEIVED)).getValue() >= sleepTimeMS);
  }

  @Test
  public void testConnectorPollMetrics() {
    CONNECTOR_CONSUMER1.createPollMetrics();
    CONNECTOR_CONSUMER2.createPollMetrics();

    for (int i = 0; i < 10; i++) {
      CONNECTOR_CONSUMER1.updateNumKafkaPolls(1);
      CONNECTOR_CONSUMER2.updateNumKafkaPolls(5);
      CONNECTOR_CONSUMER1.updateEventCountsPerPoll(1 * i);
      CONNECTOR_CONSUMER2.updateEventCountsPerPoll(5 * i);
      CONNECTOR_CONSUMER1.updateClientPollOverTimeout(1);
      CONNECTOR_CONSUMER2.updateClientPollOverTimeout(2);
      CONNECTOR_CONSUMER1.updateClientPollIntervalOverSessionTimeout(3);
      CONNECTOR_CONSUMER2.updateClientPollIntervalOverSessionTimeout(4);
    }
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.NUM_KAFKA_POLLS)).getCount(), 10);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.NUM_KAFKA_POLLS)).getCount(), 50);
    Assert.assertEquals(((Histogram) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.EVENT_COUNTS_PER_POLL)).getSnapshot().get99thPercentile(), 9.0);
    Assert.assertEquals(((Histogram) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.EVENT_COUNTS_PER_POLL)).getSnapshot().get99thPercentile(), 45.0);
    Assert.assertEquals(((Counter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.CLIENT_POLL_OVER_TIMEOUT)).getCount(), 30);
    Assert.assertEquals(((Counter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT)).getCount(), 70);
  }


  @Test
  public void testPConnectorPartitionMetrics() {
    CONNECTOR_CONSUMER1.createPartitionMetrics();
    CONNECTOR_CONSUMER2.createPartitionMetrics();

    for (int i = 0; i < 5; i++) {
      CONNECTOR_CONSUMER1.updateRebalanceRate(1);
      CONNECTOR_CONSUMER2.updateRebalanceRate(2);
    }
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.REBALANCE_RATE)).getCount(), 5);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.REBALANCE_RATE)).getCount(), 10);
    Assert.assertEquals(((Meter) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.REBALANCE_RATE)).getCount(), 15);

    CONNECTOR_CONSUMER1.updateStuckPartitions(10);
    CONNECTOR_CONSUMER2.updateStuckPartitions(20);

    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 10);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 20);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 30);
    CONNECTOR_CONSUMER1.updateStuckPartitions(5);
    CONNECTOR_CONSUMER2.updateStuckPartitions(12);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 5);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 12);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 17);

    CONNECTOR_CONSUMER1.updateStuckPartitions(0);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 0);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 12);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 12);

    CONNECTOR_CONSUMER2.resetStuckPartitions();
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER1_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 0);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_CONSUMER2_NAME + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 0);
    Assert.assertEquals((long) ((Gauge) METRICS_MANAGER.getMetric(CLASS_NAME +
        DELIMITED_AGGREGATE + CommonConnectorMetrics.STUCK_PARTITIONS)).getValue(), 0);
  }
}
