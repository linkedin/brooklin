/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.lang.reflect.Method;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.metrics.DynamicMetricsManager;


/**
 * Tests for {@link KafkaBasedConnectorTaskMetrics}
 */
public class TestKafkaBasedConnectorTaskMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaBasedConnectorTaskMetrics.class);
  private static final String CONSUMER1_NAME = "CONNECTOR_CONSUMER1";
  private static final String DELIMITED_CONSUMER1_NAME = "." + CONSUMER1_NAME + ".";
  private static final String CONSUMER2_NAME = "CONNECTOR_CONSUMER2";
  private static final String DELIMITED_CONSUMER2_NAME = "." + CONSUMER2_NAME + ".";
  private static final String DELIMITED_AGGREGATE_NAME = ".aggregate.";
  private static final String CLASS_NAME = TestKafkaBasedConnectorTaskMetrics.class.getName();

  private DynamicMetricsManager _metricsManager;

  @BeforeMethod
  public void setup(Method method) {
    _metricsManager = DynamicMetricsManager.createInstance(new MetricRegistry(), method.getName());
  }

  @Test
  public void testConnectorMetrics() {
    KafkaBasedConnectorTaskMetrics connectorConsumer1 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER1_NAME, LOG, true);
    KafkaBasedConnectorTaskMetrics connectorConsumer2 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER2_NAME, LOG, true);
    KafkaBasedConnectorTaskMetrics connectorConsumer3 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER1_NAME, LOG, true);

    long consumer1NumConfigPausedPartitions;
    long consumer1NumAutoPausedPartitionsOnInFlightMessages;
    long consumer1NumAutoPausedPartitionsOnError;
    long consumer2NumConfigPausedPartitions;
    long consumer2NumAutoPausedPartitionsOnInFlightMessages;
    long consumer2NumAutoPausedPartitionsOnError;
    long consumer3NumConfigPausedPartitions;
    long consumer3NumAutoPausedPartitionsOnInFlightMessages;
    long consumer3NumAutoPausedPartitionsOnError;

    connectorConsumer1.createPollMetrics();
    connectorConsumer2.createPollMetrics();

    connectorConsumer1.updateNumPolls(5);
    connectorConsumer1.updateEventCountsPerPoll(10);
    connectorConsumer1.updateEventCountsPerPoll(40);
    connectorConsumer1.updatePollDurationMs(50000);
    consumer1NumConfigPausedPartitions = 5;
    consumer1NumAutoPausedPartitionsOnInFlightMessages = 15;
    consumer1NumAutoPausedPartitionsOnError = 25;
    consumer2NumConfigPausedPartitions = 15;
    consumer2NumAutoPausedPartitionsOnInFlightMessages = 25;
    consumer2NumAutoPausedPartitionsOnError = 35;
    connectorConsumer1.updateNumConfigPausedPartitions(consumer1NumConfigPausedPartitions);
    connectorConsumer1.updateNumAutoPausedPartitionsOnInFlightMessages(consumer1NumAutoPausedPartitionsOnInFlightMessages);
    connectorConsumer1.updateNumAutoPausedPartitionsOnError(consumer1NumAutoPausedPartitionsOnError);
    connectorConsumer2.updateNumConfigPausedPartitions(consumer2NumConfigPausedPartitions);
    connectorConsumer2.updateNumAutoPausedPartitionsOnInFlightMessages(consumer2NumAutoPausedPartitionsOnInFlightMessages);
    connectorConsumer2.updateNumAutoPausedPartitionsOnError(consumer2NumAutoPausedPartitionsOnError);

    validatePausedPartitionsMetrics(CLASS_NAME + DELIMITED_CONSUMER1_NAME, consumer1NumConfigPausedPartitions,
        consumer1NumAutoPausedPartitionsOnError, consumer1NumAutoPausedPartitionsOnInFlightMessages);
    Assert.assertEquals(((Histogram) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + KafkaBasedConnectorTaskMetrics.POLL_DURATION_MS)).getCount(), 1);

    Assert.assertEquals(
        ((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME + "numPolls")).getCount(), 5);
    Assert.assertEquals(((Histogram) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME + "eventCountsPerPoll")).getSnapshot().getMin(), 10);
    Assert.assertEquals(((Histogram) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME + "eventCountsPerPoll")).getSnapshot().getMax(), 40);

    consumer3NumConfigPausedPartitions = 20;
    consumer3NumAutoPausedPartitionsOnInFlightMessages = 30;
    consumer3NumAutoPausedPartitionsOnError = 40;
    connectorConsumer3.updateNumConfigPausedPartitions(consumer3NumConfigPausedPartitions);
    connectorConsumer3.updateNumAutoPausedPartitionsOnInFlightMessages(consumer3NumAutoPausedPartitionsOnInFlightMessages);
    connectorConsumer3.updateNumAutoPausedPartitionsOnError(consumer3NumAutoPausedPartitionsOnError);

    validatePausedPartitionsMetrics(CLASS_NAME + DELIMITED_CONSUMER1_NAME,
        consumer1NumConfigPausedPartitions + consumer3NumConfigPausedPartitions,
        consumer1NumAutoPausedPartitionsOnError + consumer3NumAutoPausedPartitionsOnError,
        consumer1NumAutoPausedPartitionsOnInFlightMessages + consumer3NumAutoPausedPartitionsOnInFlightMessages);

    validatePausedPartitionsMetrics(CLASS_NAME + DELIMITED_CONSUMER2_NAME, consumer2NumConfigPausedPartitions,
        consumer2NumAutoPausedPartitionsOnError, consumer2NumAutoPausedPartitionsOnInFlightMessages);

    connectorConsumer3.deregisterMetrics();
    consumer3NumConfigPausedPartitions = 0;
    consumer3NumAutoPausedPartitionsOnInFlightMessages = 0;
    consumer3NumAutoPausedPartitionsOnError = 0;
    validatePausedPartitionsMetrics(CLASS_NAME + DELIMITED_CONSUMER1_NAME,
        consumer1NumConfigPausedPartitions + consumer3NumConfigPausedPartitions,
        consumer1NumAutoPausedPartitionsOnError + consumer3NumAutoPausedPartitionsOnError,
        consumer1NumAutoPausedPartitionsOnInFlightMessages + consumer3NumAutoPausedPartitionsOnInFlightMessages);

    connectorConsumer1.deregisterMetrics();
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, DELIMITED_CONSUMER1_NAME,
        KafkaBasedConnectorTaskMetrics.NUM_CONFIG_PAUSED_PARTITIONS)));
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, DELIMITED_CONSUMER1_NAME,
        KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES)));
    Assert.assertNull(_metricsManager.getMetric(String.join(".", CLASS_NAME, DELIMITED_CONSUMER1_NAME,
        KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR)));

    KafkaBasedConnectorTaskMetrics connectorConsumer4 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER1_NAME, LOG, true);

    validatePausedPartitionsMetrics(CLASS_NAME + DELIMITED_CONSUMER1_NAME, 0, 0, 0);
    connectorConsumer4.deregisterMetrics();
  }

  private void validatePausedPartitionsMetrics(String metricsPrefix, long numConfigPausedPartitions,
      long numAutoPausedPartitionsOnError, long  numAutoPausedPartitionsOnInflightMessages) {
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(
        metricsPrefix + KafkaBasedConnectorTaskMetrics.NUM_CONFIG_PAUSED_PARTITIONS)).getValue(),
        numConfigPausedPartitions);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(
        metricsPrefix + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR)).getValue(),
        numAutoPausedPartitionsOnError);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(
        metricsPrefix + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES)).getValue(),
        numAutoPausedPartitionsOnInflightMessages);
  }

  @Test
  public void testConnectorPartitionMetrics() {
    KafkaBasedConnectorTaskMetrics connectorConsumer1 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER1_NAME, LOG, false);
    KafkaBasedConnectorTaskMetrics connectorConsumer2 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER2_NAME, LOG, false);

    connectorConsumer1.createPartitionMetrics();
    connectorConsumer2.createPartitionMetrics();

    Random random = new Random();

    long consumer1NumTopics = random.nextInt(1000);
    long consumer1NumPartitions = consumer1NumTopics * random.nextInt(1024);
    long consumer2NumTopics = random.nextInt(1000);
    long consumer2NumPartitions = consumer2NumTopics * random.nextInt(1024);

    connectorConsumer1.updateNumTopics(consumer1NumTopics);
    connectorConsumer1.updateNumPartitions(consumer1NumPartitions);
    connectorConsumer2.updateNumTopics(consumer2NumTopics);
    connectorConsumer2.updateNumPartitions(consumer2NumPartitions);
    validateNumTopicsAndPartitions(CLASS_NAME + DELIMITED_CONSUMER1_NAME, consumer1NumTopics, consumer1NumPartitions);
    validateNumTopicsAndPartitions(CLASS_NAME + DELIMITED_CONSUMER2_NAME, consumer2NumTopics, consumer2NumPartitions);
    validateNumTopicsAndPartitions(CLASS_NAME + DELIMITED_AGGREGATE_NAME,
        consumer1NumTopics + consumer2NumTopics, consumer1NumPartitions + consumer2NumPartitions);

    // add more topics and partitions
    consumer1NumTopics += random.nextInt(1000);
    consumer1NumPartitions = consumer1NumTopics * random.nextInt(1024);
    consumer2NumTopics += random.nextInt(1000);
    consumer2NumPartitions = consumer2NumTopics * random.nextInt(1024);

    connectorConsumer1.updateNumTopics(consumer1NumTopics);
    connectorConsumer1.updateNumPartitions(consumer1NumPartitions);
    connectorConsumer2.updateNumTopics(consumer2NumTopics);
    connectorConsumer2.updateNumPartitions(consumer2NumPartitions);
    validateNumTopicsAndPartitions(CLASS_NAME + DELIMITED_CONSUMER1_NAME, consumer1NumTopics, consumer1NumPartitions);
    validateNumTopicsAndPartitions(CLASS_NAME + DELIMITED_CONSUMER2_NAME, consumer2NumTopics, consumer2NumPartitions);
    validateNumTopicsAndPartitions(CLASS_NAME + DELIMITED_AGGREGATE_NAME,
        consumer1NumTopics + consumer2NumTopics, consumer1NumPartitions + consumer2NumPartitions);
  }

  private void validateNumTopicsAndPartitions(String metricsPrefix, long numTopics, long numPartitions) {
    Assert.assertEquals(
        (long) ((Gauge) _metricsManager.getMetric(metricsPrefix + "numTopics")).getValue(), numTopics);
    Assert.assertEquals(
        (long) ((Gauge) _metricsManager.getMetric(metricsPrefix + "numPartitions")).getValue(), numPartitions);
  }
}


