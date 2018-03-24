package com.linkedin.datastream.metrics;

import java.lang.reflect.Method;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;


@Test
public class TestDynamicMetricsManager {
  private DynamicMetricsManager _metricsManager;

  private static final String CLASS_NAME = TestDynamicMetricsManager.class.getName();

  @BeforeMethod
  public void setup(Method method) {
    _metricsManager = DynamicMetricsManager.createInstance(new MetricRegistry(), method.getName());
  }

  @Test
  public void testCreateOrUpdateSlidingWindowHistogram() throws Exception {
    long highLatency = 10000;
    _metricsManager.createOrUpdateSlidingWindowHistogram(CLASS_NAME, "test", "latency", 1, highLatency);
    // definitely not ideal. but using Clock makes the API itself looks clumsy
    // So keep using the system clock for now and if we see flakiness or something we can make changes
    Thread.sleep(100);
    _metricsManager.createOrUpdateSlidingWindowHistogram(CLASS_NAME, "test", "latency", 1, 10);
    String fullName = MetricRegistry.name(CLASS_NAME, "test", "latency");
    Histogram histogram = _metricsManager.getMetric(fullName);
    // verify that we are not stuck in the old latency value
    Assert.assertNotEquals(histogram.getSnapshot().getMax(), highLatency);
  }
}
