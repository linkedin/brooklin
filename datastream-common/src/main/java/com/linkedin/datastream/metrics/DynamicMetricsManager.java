package com.linkedin.datastream.metrics;

import org.apache.commons.lang.Validate;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;


/**
 * Manages dynamic metrics and supports creating/updating metrics on the fly.
 */
public class DynamicMetricsManager {

  private static DynamicMetricsManager _instance = null;
  private final MetricRegistry _metricRegistry;

  private DynamicMetricsManager(MetricRegistry metricRegistry) {
    _metricRegistry = metricRegistry;
  }

  public static DynamicMetricsManager createInstance(MetricRegistry metricRegistry) {
    if (_instance == null) {
      _instance = new DynamicMetricsManager(metricRegistry);
    }
    return _instance;
  }

  public static DynamicMetricsManager getInstance() {
    if (_instance == null) {
      throw new IllegalStateException("DynamicMetricsManager has not yet been instantiated.");
    }
    return _instance;
  }

  /**
   * Register the metric for the specified key/metricName pair by the given value; if it has
   * already been registered, do nothing
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param metric the metric to be registered
   */
  public synchronized void registerMetric(Class<?> clazz, String key, String metricName, Metric metric) {
    validateArguments(clazz, key, metricName);

    String fullMetricName = MetricRegistry.name(clazz.getSimpleName(), key, metricName);

    // create and register the metric if it does not exist
    if (!_metricRegistry.getMetrics().containsKey(fullMetricName)) {
      _metricRegistry.register(fullMetricName, metric);
    }
  }

  /**
   * Update the counter (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * To decrement the counter, pass in a negative value.
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value amount to increment the counter by (use negative value to decrement)
   */
  public synchronized void createOrUpdateCounter(Class<?> clazz, String key, String metricName, long value) {
    validateArguments(clazz, key, metricName);

    String fullMetricName = MetricRegistry.name(clazz.getSimpleName(), key, metricName);

    // create and register the metric if it does not exist
    Counter counter = _metricRegistry.getCounters().get(fullMetricName);
    if (counter == null) {
      counter = _metricRegistry.counter(fullMetricName);
    }
    counter.inc(value);
  }

  /**
   * Update the meter (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value the value to mark on the meter
   */
  public synchronized void createOrUpdateMeter(Class<?> clazz, String key, String metricName, long value) {
    validateArguments(clazz, key, metricName);

    String fullMetricName = MetricRegistry.name(clazz.getSimpleName(), key, metricName);

    // create and register the metric if it does not exist
    Meter meter = _metricRegistry.getMeters().get(fullMetricName);
    if (meter == null) {
      meter = _metricRegistry.meter(fullMetricName);
    }
    meter.mark(value);
  }

  /**
   * Update the histogram (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value the value to update on the histogram
   */
  public synchronized void createOrUpdateHistogram(Class<?> clazz, String key, String metricName, long value) {
    validateArguments(clazz, key, metricName);
    String fullMetricName = MetricRegistry.name(clazz.getSimpleName(), key, metricName);

    // create and register the metric if it does not exist
    Histogram histogram = _metricRegistry.getHistograms().get(fullMetricName);
    if (histogram == null) {
      histogram = _metricRegistry.histogram(fullMetricName);
    }
    histogram.update(value);
  }

  private void validateArguments(Class<?> clazz, String key, String metricName) {
    Validate.notNull(clazz, "clazz argument is null.");
    Validate.notNull(key, "key argument is null.");
    Validate.notNull(metricName, "metricName argument is null.");
  }

}
