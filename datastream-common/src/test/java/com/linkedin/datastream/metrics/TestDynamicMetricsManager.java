package com.linkedin.datastream.metrics;

import java.lang.reflect.Method;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;


@Test
public class TestDynamicMetricsManager {
  private static final String CLASS_NAME = TestDynamicMetricsManager.class.getSimpleName();

  private DynamicMetricsManager _metricsManager;

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

  public void testCreateOrUpdateCounter() {
    String numEvents = "numEvents";
    String numErrors = "numErrors";

    // create key-less counter
    String fullKeylessMetricName = MetricRegistry.name(CLASS_NAME, numEvents);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, numEvents, 1);
    Counter keylessCounter = _metricsManager.getMetric(fullKeylessMetricName);
    Assert.assertEquals(keylessCounter.getCount(), 1);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, numEvents, 1);
    Assert.assertEquals(keylessCounter.getCount(), 2);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, numEvents, 5);

    // create keyed counters
    String someKey = "someKey";
    String fullMetricName = MetricRegistry.name(CLASS_NAME, someKey, numEvents);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, someKey, numEvents, 1);
    Counter keyedCounter = _metricsManager.getMetric(fullMetricName);
    Assert.assertEquals(keyedCounter.getCount(), 1);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, someKey, numEvents, 1);
    Assert.assertEquals(keyedCounter.getCount(), 2);

    _metricsManager.createOrUpdateCounter(CLASS_NAME, someKey, numErrors, 19);
    Counter counter = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, someKey, numErrors));
    Assert.assertEquals(counter.getCount(), 19);

    Assert.assertNotEquals(keyedCounter, keylessCounter);
    Assert.assertNotEquals(keyedCounter, counter);

    // create another key-less counter
    String anotherFullKeylessMetricName = MetricRegistry.name(CLASS_NAME, numErrors);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, numErrors, 1);
    Counter anotherKeylessCounter = _metricsManager.getMetric(anotherFullKeylessMetricName);
    Assert.assertEquals(anotherKeylessCounter.getCount(), 1);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, numErrors, 1);
    Assert.assertEquals(anotherKeylessCounter.getCount(), 2);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, numErrors, 5);

    Assert.assertNotEquals(anotherKeylessCounter, keylessCounter);

    // create another keyed counter
    String anotherKey = "anotherKey";
    String anotherFullMetricName = MetricRegistry.name(CLASS_NAME, anotherKey, numErrors);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, anotherKey, numErrors, 1);
    Counter anotherKeyedCounter = _metricsManager.getMetric(anotherFullMetricName);
    Assert.assertEquals(anotherKeyedCounter.getCount(), 1);
    _metricsManager.createOrUpdateCounter(CLASS_NAME, anotherKey, numErrors, 1);
    Assert.assertEquals(anotherKeyedCounter.getCount(), 2);

    Assert.assertNotEquals(keyedCounter, anotherKeyedCounter);

    // unregister and check cache
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, null, numErrors).isPresent());
    _metricsManager.unregisterMetric(CLASS_NAME, numErrors);
    Assert.assertFalse(_metricsManager.checkCache(CLASS_NAME, null, numErrors).isPresent());
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, anotherKey, numErrors).isPresent());
    _metricsManager.unregisterMetric(CLASS_NAME, anotherKey, numErrors);
    Assert.assertFalse(_metricsManager.checkCache(CLASS_NAME, anotherKey, numErrors).isPresent());

    // others remain in cache
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, null, numEvents).isPresent());
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, someKey, numEvents).isPresent());
  }

  public void testCreateOrUpdateMeter() {
    String eventRate = "eventRate";
    String errorRate = "errorRate";

    // create key-less meter
    String fullKeylessMetricName = MetricRegistry.name(CLASS_NAME, eventRate);
    _metricsManager.createOrUpdateMeter(CLASS_NAME, eventRate, 1);
    Meter keylessMeter = _metricsManager.getMetric(fullKeylessMetricName);
    Assert.assertEquals(keylessMeter.getCount(), 1);
    _metricsManager.createOrUpdateMeter(CLASS_NAME, eventRate, 1);
    Assert.assertEquals(keylessMeter.getCount(), 2);
    _metricsManager.createOrUpdateMeter(CLASS_NAME, eventRate, 5);

    // create keyed meters
    String someKey = "someKey";
    String fullMetricName = MetricRegistry.name(CLASS_NAME, someKey, eventRate);
    _metricsManager.createOrUpdateMeter(CLASS_NAME, someKey, eventRate, 1);
    Meter keyedMeter = _metricsManager.getMetric(fullMetricName);
    Assert.assertEquals(keyedMeter.getCount(), 1);
    _metricsManager.createOrUpdateMeter(CLASS_NAME, someKey, eventRate, 1);
    Assert.assertEquals(keyedMeter.getCount(), 2);

    _metricsManager.createOrUpdateMeter(CLASS_NAME, someKey, errorRate, 19);
    Meter meter = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, someKey, errorRate));
    Assert.assertEquals(meter.getCount(), 19);

    Assert.assertNotEquals(keyedMeter, keylessMeter);
    Assert.assertNotEquals(keyedMeter, meter);

    // create another key-less meter
    String anotherFullKeylessMetricName = MetricRegistry.name(CLASS_NAME, errorRate);
    _metricsManager.createOrUpdateMeter(CLASS_NAME, errorRate, 11);
    Meter anotherKeylessMeter = _metricsManager.getMetric(anotherFullKeylessMetricName);
    Assert.assertEquals(anotherKeylessMeter.getCount(), 11);
    Assert.assertNotEquals(anotherKeylessMeter, keylessMeter);

    // create another keyed meter
    String anotherKey = "anotherKey";
    String anotherFullKeyedMetricName = MetricRegistry.name(CLASS_NAME, anotherKey, errorRate);
    _metricsManager.createOrUpdateMeter(CLASS_NAME, anotherKey, errorRate, 6);
    Meter anotherKeyedMeter = _metricsManager.getMetric(anotherFullKeyedMetricName);
    Assert.assertEquals(anotherKeyedMeter.getCount(), 6);
    Assert.assertNotEquals(anotherKeyedMeter, keyedMeter);

    // unregister and check cache
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, null, errorRate).isPresent());
    _metricsManager.unregisterMetric(CLASS_NAME, errorRate);
    Assert.assertFalse(_metricsManager.checkCache(CLASS_NAME, null, errorRate).isPresent());
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, anotherKey, errorRate).isPresent());
    _metricsManager.unregisterMetric(CLASS_NAME, anotherKey, errorRate);
    Assert.assertFalse(_metricsManager.checkCache(CLASS_NAME, anotherKey, errorRate).isPresent());

    // others remain in cache
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, null, eventRate).isPresent());
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, someKey, eventRate).isPresent());
  }

  public void testCreateOrUpdateHistogram() {
    String eventLatency = "eventLatency";
    String processLatency = "processLatency";

    // create key-less histogram
    String fullKeylessMetricName = MetricRegistry.name(CLASS_NAME, eventLatency);
    _metricsManager.createOrUpdateHistogram(CLASS_NAME, eventLatency, 1);
    Histogram keylessHistogram = _metricsManager.getMetric(fullKeylessMetricName);
    Assert.assertEquals(keylessHistogram.getSnapshot().getMin(), 1);
    _metricsManager.createOrUpdateHistogram(CLASS_NAME, eventLatency, 5);
    Assert.assertEquals(keylessHistogram.getSnapshot().getMax(), 5);
    _metricsManager.createOrUpdateHistogram(CLASS_NAME, eventLatency, 10);

    // create keyed histogram
    String someKey = "someKey";
    String fullMetricName = MetricRegistry.name(CLASS_NAME, someKey, eventLatency);
    _metricsManager.createOrUpdateHistogram(CLASS_NAME, someKey, eventLatency, 3);
    Histogram keyedHistogram = _metricsManager.getMetric(fullMetricName);
    Assert.assertEquals(keyedHistogram.getSnapshot().getMin(), 3);
    _metricsManager.createOrUpdateHistogram(CLASS_NAME, someKey, eventLatency, 6);
    Assert.assertEquals(keyedHistogram.getSnapshot().getMax(), 6);

    _metricsManager.createOrUpdateHistogram(CLASS_NAME, someKey, processLatency, 19);
    Histogram histogram = _metricsManager.getMetric(MetricRegistry.name(CLASS_NAME, someKey, processLatency));
    Assert.assertEquals(histogram.getSnapshot().getMin(), 19);

    Assert.assertNotEquals(keyedHistogram, keylessHistogram);
    Assert.assertNotEquals(keyedHistogram, histogram);

    // create another key-less histogram
    String anotherFullKeylessMetricName = MetricRegistry.name(CLASS_NAME, processLatency);
    _metricsManager.createOrUpdateHistogram(CLASS_NAME, processLatency, 11);
    Histogram anotherKeylessHistogram = _metricsManager.getMetric(anotherFullKeylessMetricName);
    Assert.assertEquals(anotherKeylessHistogram.getSnapshot().getMax(), 11);
    Assert.assertNotEquals(anotherKeylessHistogram, keylessHistogram);

    // create another keyed histogram
    String anotherKey = "anotherKey";
    String anotherFullKeyedMetricName = MetricRegistry.name(CLASS_NAME, anotherKey, processLatency);
    _metricsManager.createOrUpdateHistogram(CLASS_NAME, anotherKey, processLatency, 6);
    Histogram anotherKeyedHistogram = _metricsManager.getMetric(anotherFullKeyedMetricName);
    Assert.assertEquals(anotherKeyedHistogram.getSnapshot().getMin(), 6);
    Assert.assertNotEquals(anotherKeyedHistogram, keyedHistogram);

    // unregister and check cache
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, null, processLatency).isPresent());
    _metricsManager.unregisterMetric(CLASS_NAME, processLatency);
    Assert.assertFalse(_metricsManager.checkCache(CLASS_NAME, null, processLatency).isPresent());
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, anotherKey, processLatency).isPresent());
    _metricsManager.unregisterMetric(CLASS_NAME, anotherKey, processLatency);
    Assert.assertFalse(_metricsManager.checkCache(CLASS_NAME, anotherKey, processLatency).isPresent());

    // others remain in cache
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, null, eventLatency).isPresent());
    Assert.assertTrue(_metricsManager.checkCache(CLASS_NAME, someKey, eventLatency).isPresent());
  }
}
