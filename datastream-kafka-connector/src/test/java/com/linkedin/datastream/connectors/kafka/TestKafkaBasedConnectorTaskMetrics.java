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
  public void testConnectorPollMetrics() {
    KafkaBasedConnectorTaskMetrics connectorConsumer1 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER1_NAME, LOG);
    KafkaBasedConnectorTaskMetrics connectorConsumer2 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER2_NAME, LOG);

    connectorConsumer1.createPollMetrics();
    connectorConsumer2.createPollMetrics();

    connectorConsumer1.updateNumPolls(5);
    connectorConsumer1.updateEventCountsPerPoll(10);
    connectorConsumer1.updateEventCountsPerPoll(40);
    connectorConsumer2.updateNumConfigPausedPartitions(15);
    connectorConsumer2.updateNumAutoPausedPartitionsOnInFlightMessages(25);
    connectorConsumer2.updateNumAutoPausedPartitionsOnError(35);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_CONFIG_PAUSED_PARTITIONS)).getValue(), 0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR)).getValue(), 0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES)).getValue(), 0);

    Assert.assertEquals(
        ((Meter) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME + "numPolls")).getCount(), 5);
    Assert.assertEquals(((Histogram) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME + "eventCountsPerPoll")).getSnapshot().getMin(), 10);
    Assert.assertEquals(((Histogram) _metricsManager.getMetric(
        CLASS_NAME + DELIMITED_CONSUMER1_NAME + "eventCountsPerPoll")).getSnapshot().getMax(), 40);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_CONFIG_PAUSED_PARTITIONS)).getValue(), 15);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES)).getValue(), 25);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR)).getValue(), 35);
  }

  @Test
  public void testConnectorPartitionMetrics() {
    KafkaBasedConnectorTaskMetrics connectorConsumer1 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER1_NAME, LOG);
    KafkaBasedConnectorTaskMetrics connectorConsumer2 =
        new KafkaBasedConnectorTaskMetrics(CLASS_NAME, CONSUMER2_NAME, LOG);

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

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + "numTopics")).getValue(), consumer1NumTopics);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + "numPartitions")).getValue(), consumer1NumPartitions);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + "numTopics")).getValue(), consumer2NumTopics);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + "numPartitions")).getValue(), consumer2NumPartitions);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE_NAME
        + "numTopics")).getValue(), consumer1NumTopics + consumer2NumTopics);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE_NAME
        + "numPartitions")).getValue(), consumer1NumPartitions + consumer2NumPartitions);

    // add more topics and partitions
    consumer1NumTopics += random.nextInt(1000);
    consumer1NumPartitions = consumer1NumTopics * random.nextInt(1024);
    consumer2NumTopics += random.nextInt(1000);
    consumer2NumPartitions = consumer2NumTopics * random.nextInt(1024);

    connectorConsumer1.updateNumTopics(consumer1NumTopics);
    connectorConsumer1.updateNumPartitions(consumer1NumPartitions);
    connectorConsumer2.updateNumTopics(consumer2NumTopics);
    connectorConsumer2.updateNumPartitions(consumer2NumPartitions);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + "numTopics")).getValue(), consumer1NumTopics);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + "numPartitions")).getValue(), consumer1NumPartitions);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + "numTopics")).getValue(), consumer2NumTopics);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + "numPartitions")).getValue(), consumer2NumPartitions);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE_NAME
        + "numTopics")).getValue(), consumer1NumTopics + consumer2NumTopics);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_AGGREGATE_NAME
        + "numPartitions")).getValue(), consumer1NumPartitions + consumer2NumPartitions);
  }
}


