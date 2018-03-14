package com.linkedin.datastream.connectors.kafka;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import java.lang.reflect.Method;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class TestKafkaBasedConnectorTaskMetrics {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaBasedConnectorTaskMetrics.class);
  private static final String CONSUMER1_NAME = "CONNECTOR_CONSUMER1";
  private static final String DELIMITED_CONSUMER1_NAME = "." + CONSUMER1_NAME + ".";
  private static final String CONSUMER2_NAME = "CONNECTOR_CONSUMER2";
  private static final String DELIMITED_CONSUMER2_NAME = "." + CONSUMER2_NAME + ".";
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

    connectorConsumer2.updateNumConfigPausedPartitions(15);
    connectorConsumer2.updateNumAutoPausedPartitionsOnInFlightMessages(25);
    connectorConsumer2.updateNumAutoPausedPartitionsOnError(35);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_CONFIG_PAUSED_PARTITIONS)).getValue(), 0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR)).getValue(), 0);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER1_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES)).getValue(), 0);

    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_CONFIG_PAUSED_PARTITIONS)).getValue(), 15);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES)).getValue(), 25);
    Assert.assertEquals((long) ((Gauge) _metricsManager.getMetric(CLASS_NAME + DELIMITED_CONSUMER2_NAME
        + KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR)).getValue(), 35);
  }
}


