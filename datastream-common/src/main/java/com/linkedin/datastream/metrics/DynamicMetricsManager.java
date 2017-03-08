package com.linkedin.datastream.metrics;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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

  // Metrics indexed by simple class name
  // Simple class name -> full metric name -> metric
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>> _indexedMetrics;

  private DynamicMetricsManager(MetricRegistry metricRegistry) {
    _metricRegistry = metricRegistry;
    _indexedMetrics = new ConcurrentHashMap<>();
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

  private Optional<Metric> checkCache(String simpleClassName, String fullMetricName) {
    return Optional.ofNullable(getClassMetrics(simpleClassName).get(fullMetricName));
  }

  private void updateCache(String simpleClassName, String fullMetricName, Metric metric) {
    getClassMetrics(simpleClassName).putIfAbsent(fullMetricName, metric);
  }

  private ConcurrentHashMap<String, Metric> getClassMetrics(String simpleClassName) {
    return _indexedMetrics.computeIfAbsent(simpleClassName, k -> new ConcurrentHashMap<>());
  }

  /**
   * Register the metric for the specified key/metricName pair by the given value; if it has
   * already been registered, do nothing
   * @param classSimpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param metric the metric to be registered
   */
  public void registerMetric(String classSimpleName, String key, String metricName, Metric metric) {
    validateArguments(classSimpleName, metricName);
    Validate.notNull(metric, "metric argument is null.");

    String fullMetricName = MetricRegistry.name(classSimpleName, key, metricName);

    if (!checkCache(classSimpleName, fullMetricName).isPresent()) {
      // create and register the metric if it does not exist
      if (!_metricRegistry.getMetrics().containsKey(fullMetricName)) {
        try {
          _metricRegistry.register(fullMetricName, metric);
        } catch (IllegalArgumentException e) {
          // Ignore error, metric already register.
        }
      }

      updateCache(classSimpleName, fullMetricName, metric);
    }
  }

  /**
   * Register the metric for the specified metricName; if it has already been registered, do nothing
   * @param classSimpleName the simple name of the underlying class
   * @param metricName the metric name
   * @param metric the metric to be registered
   */
  public void registerMetric(String classSimpleName, String metricName, Metric metric) {
    registerMetric(classSimpleName, null, metricName, metric);
  }

  /**
   * Register the metric for the specified key/metricName pair by the given value; if it has
   * already been registered, do nothing
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param metric the metric to be registered
   */
  public void registerMetric(Class<?> clazz, String key, String metricName, Metric metric) {
    registerMetric(clazz.getSimpleName(), key, metricName, metric);
  }

  /**
   * Register the metric for the specified metricName; if it has already been registered, do nothing
   * @param clazz the class containing the metric
   * @param metricName the metric name
   * @param metric the metric to be registered
   */
  public void registerMetric(Class<?> clazz, String metricName, Metric metric) {
    registerMetric(clazz, null, metricName, metric);
  }

  /**
   * Update the counter (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * To decrement the counter, pass in a negative value.
   * @param classSimpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value amount to increment the counter by (use negative value to decrement)
   */
  public void createOrUpdateCounter(String classSimpleName, String key, String metricName, long value) {
    validateArguments(classSimpleName, metricName);

    String fullMetricName = MetricRegistry.name(classSimpleName, key, metricName);

    // create and register the metric if it does not exist
    Counter counter =
        (Counter) checkCache(classSimpleName, fullMetricName).orElse(_metricRegistry.counter(fullMetricName));
    counter.inc(value);
    updateCache(classSimpleName, fullMetricName, counter);
  }

  /**
   * Update the counter (or creates it if it does not exist) for the specified metricName.
   * To decrement the counter, pass in a negative value.
   * @param classSimpleName the simple name of the underlying class
   * @param metricName the metric name
   * @param value amount to increment the counter by (use negative value to decrement)
   */
  public void createOrUpdateCounter(String classSimpleName, String metricName, long value) {
    createOrUpdateCounter(classSimpleName, null, metricName, value);
  }

  /**
   * Update the counter (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * To decrement the counter, pass in a negative value.
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value amount to increment the counter by (use negative value to decrement)
   */
  public void createOrUpdateCounter(Class<?> clazz, String key, String metricName, long value) {
    createOrUpdateCounter(clazz.getSimpleName(), key, metricName, value);
  }

  /**
   * Update the counter (or creates it if it does not exist) for the specified metricName.
   * To decrement the counter, pass in a negative value.
   * @param clazz the class containing the metric
   * @param metricName the metric name
   * @param value amount to increment the counter by (use negative value to decrement)
   */
  public void createOrUpdateCounter(Class<?> clazz, String metricName, long value) {
    createOrUpdateCounter(clazz, null, metricName, value);
  }

  /**
   * Update the meter (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * @param classSimpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value the value to mark on the meter
   */
  public void createOrUpdateMeter(String classSimpleName, String key, String metricName, long value) {
    validateArguments(classSimpleName, metricName);

    String fullMetricName = MetricRegistry.name(classSimpleName, key, metricName);

    // create and register the metric if it does not exist
    Meter meter = (Meter) checkCache(classSimpleName, fullMetricName).orElse(_metricRegistry.meter(fullMetricName));

    meter.mark(value);
    updateCache(classSimpleName, fullMetricName, meter);
  }

  /**
   * Update the meter (or creates it if it does not exist) for the specified metricName.
   * @param classSimpleName the simple name of the underlying class
   * @param metricName the metric name
   * @param value the value to mark on the meter
   */
  public void createOrUpdateMeter(String classSimpleName, String metricName, long value) {
    createOrUpdateMeter(classSimpleName, null, metricName, value);
  }

  /**
   * Update the meter (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value the value to mark on the meter
   */
  public void createOrUpdateMeter(Class<?> clazz, String key, String metricName, long value) {
    createOrUpdateMeter(clazz.getSimpleName(), key, metricName, value);
  }

  /**
   * Update the meter (or creates it if it does not exist) for the specified metricName.
   * @param clazz the class containing the metric
   * @param metricName the metric name
   * @param value the value to mark on the meter
   */
  public void createOrUpdateMeter(Class<?> clazz, String metricName, long value) {
    createOrUpdateMeter(clazz, null, metricName, value);
  }

  /**
   * Update the histogram (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * @param classSimpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value the value to update on the histogram
   */
  public void createOrUpdateHistogram(String classSimpleName, String key, String metricName, long value) {
    validateArguments(classSimpleName, metricName);
    String fullMetricName = MetricRegistry.name(classSimpleName, key, metricName);

    // create and register the metric if it does not exist
    Histogram histogram =
        (Histogram) checkCache(classSimpleName, fullMetricName).orElse(_metricRegistry.histogram(fullMetricName));
    histogram.update(value);
    updateCache(classSimpleName, fullMetricName, histogram);
  }

  /**
   * Update the histogram (or creates it if it does not exist) for the specified metricName.
   * @param classSimpleName the simple name of the underlying class
   * @param metricName the metric name
   * @param value the value to update on the histogram
   */
  public void createOrUpdateHistogram(String classSimpleName, String metricName, long value) {
    createOrUpdateHistogram(classSimpleName, null, metricName, value);
  }

  /**
   * Update the histogram (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * @param clazz the class containing the metric
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param value the value to update on the histogram
   */
  public void createOrUpdateHistogram(Class<?> clazz, String key, String metricName, long value) {
    createOrUpdateHistogram(clazz.getSimpleName(), key, metricName, value);
  }

  /**
   * Update the histogram (or creates it if it does not exist) for the specified metricName.
   * @param clazz the class containing the metric
   * @param metricName the metric name
   * @param value the value to update on the histogram
   */
  public void createOrUpdateHistogram(Class<?> clazz, String metricName, long value) {
    createOrUpdateHistogram(clazz, null, metricName, value);
  }

  /**
   * Get the metric object by name of the specified type based on return value.
   * Currently only used by test cases.
   * @param name
   * @param <T>
   * @return
   */
  @SuppressWarnings("unchecked")
  public <T extends Metric> T getMetric(String name) {
    return (T) _metricRegistry.getMetrics().getOrDefault(name, null);
  }

  private void validateArguments(String classSimpleName, String metricName) {
    Validate.notNull(classSimpleName, "classSimpleName argument is null.");
    Validate.notNull(metricName, "metricName argument is null.");
  }
}
