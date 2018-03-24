package com.linkedin.datastream.metrics;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;
import com.codahale.metrics.MetricRegistry;
import javax.annotation.Nullable;


/**
 * Manages dynamic metrics and supports creating/updating metrics on the fly.
 */
public class DynamicMetricsManager {
  private static final Logger LOG = LoggerFactory.getLogger(DynamicMetricsManager.class);

  private static DynamicMetricsManager _instance = null;
  private MetricRegistry _metricRegistry;

  // Metrics indexed by simple class name
  // Simple class name -> full metric name -> metric
  // The map helps reducing contention on the internal map of MetricRegistry.
  // This is created solely for the createOrUpdate APIs, not by registerMetric
  // because the former can be called repeatedly to update the metric whereas
  // the latter is typically only called once per metric during initialization.
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>> _indexedMetrics;

  private DynamicMetricsManager(MetricRegistry metricRegistry) {
    _metricRegistry = metricRegistry;
    _indexedMetrics = new ConcurrentHashMap<>();
  }

  /**
   * Instantiate the singleton of DynamicMetricsManager; This is a no-op if it already exists.
   * @param metricRegistry
   * @return
   */
  public static DynamicMetricsManager createInstance(MetricRegistry metricRegistry) {
    return createInstance(metricRegistry, null);
  }

  /**
   * Instantiate the singleton of DynamicMetricsManager; This is a no-op if it already exists if
   * unitTests == false; otherwise, we replace the metricRegistry for unit tests to start from
   * a clean slate.
   *
   * Note that all tests validating the same metrics must be run sequentially, otherwise race
   * condition will occur.
   * @param metricRegistry
   * @param testName name of the unit test
   * @return
   */
  public static DynamicMetricsManager createInstance(MetricRegistry metricRegistry, @Nullable  String testName) {
    synchronized (DynamicMetricsManager.class) {
      if (_instance == null) {
        _instance = new DynamicMetricsManager(metricRegistry);
      }

      if (testName != null) {
        // Cannot create a new instance because callers might have already
        // cached the current instance locally which would become obsolete
        // if we swap it out behind the scene.
        _instance._metricRegistry = metricRegistry;
        _instance._indexedMetrics.clear();

        LOG.info("Returning the instance for unit test {}.", testName);
      }

      return _instance;
    }
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
   * Get a metric object based on its class. Note that codahale MetricRegistry
   * does a getOrAdd behind of scene of counter()/meter()/histogram(), etc.
   * @param name fully-qualified name of the metric
   * @param clazz class object of the metric
   * @param <T> metric type
   * @return metric object
   */
  @SuppressWarnings("unchecked")
  private <T extends Metric> T getMetric(String name, Class<T> clazz) {
    if (clazz.equals(Counter.class)) {
      return (T) _metricRegistry.counter(name);
    } else if (clazz.equals(Meter.class)) {
      return (T) _metricRegistry.meter(name);
    } else if (clazz.equals(Histogram.class)) {
      return (T) _metricRegistry.histogram(name);
    } else if (clazz.equals(Gauge.class)) {
      return (T) new ResettableGauge<>();
    } else if (clazz.equals(Timer.class)) {
      return (T) new Timer();
    } else {
      throw new IllegalArgumentException("Invalid metric type: " + clazz);
    }
  }

  /**
   * Internal method to create and register a metric with the registry. If the metric with the same
   * name has already been registered before, it will be returned instead of creating a new one as
   * metric registry forbids duplicate registrations. For an existing Gauge metric, we replace its
   * value supplier with the new supplier passed in.
   * @param simpleName namespace of the metric
   * @param key optional key for the metric (eg. source name)
   * @param metricName actual name of the metric
   * @param metricClass class of the metric type
   * @param supplier optional supplier for Gauge metric (not used for non-Gauge metrics)
   * @param <T> metric type
   * @param <V> value type for the supplier
   * @return
   */
  @SuppressWarnings("unchecked")
  private <T extends Metric, V> T doRegisterMetric(String simpleName, String key, String metricName,
      Class<T> metricClass, Supplier<V> supplier) {
    validateArguments(simpleName, metricName);
    Validate.notNull(metricClass, "metric class argument is null.");

    String fullMetricName = MetricRegistry.name(simpleName, key, metricName);

    Metric metric = getMetric(fullMetricName, metricClass);

    if (metric != null && metric instanceof ResettableGauge) {
      Validate.notNull(supplier, "null supplier to Gauge");
      ((ResettableGauge) metric).setSupplier(supplier);

      try {
        // Gauge needs explicit registration
        _metricRegistry.register(fullMetricName, metric);
      } catch (IllegalArgumentException e) {
        // This can happen with parallel unit tests
      }
    }

    // _indexedMetrics update is left to the createOrUpdate APIs which is only needed
    // if the same metrics are accessed through both registerMetric and createOrUpdate.

    return (T) metric;
  }

  /**
   * Register the metric for the specified key/metricName pair by the given value; if it has
   * already been registered, do nothing
   * @param simpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param metricClass the class object of the metric to be registered
   * @return the metric just registered or previously registered one
   */
  @SuppressWarnings("unchecked")
  public <T extends Metric> T registerMetric(String simpleName, String key, String metricName, Class<T> metricClass) {
    Validate.isTrue(!metricClass.equals(Gauge.class), "please call registerGauge() to register a Gauge metric.");
    return doRegisterMetric(simpleName, key, metricName, metricClass, null);
  }

  /**
   * Register the metric for the specified metricName; if it has already been registered, do nothing
   * @param classSimpleName the simple name of the underlying class
   * @param metricName the metric name
   * @param metricClass the metric to be registered
   * @return the metric just registered or previously registered one
   */
  public <T extends Metric> T registerMetric(String classSimpleName, String metricName, Class<T> metricClass) {
    return registerMetric(classSimpleName, null, metricName, metricClass);
  }

  /**
   * Register a Gauge metric for the specified metricName; if it has already been registered, do nothing
   * @param simpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param supplier value supplier for the Gauge
   * @return the metric just registered or previously registered one
   */
  @SuppressWarnings("unchecked")
  public <T> Gauge<T> registerGauge(String simpleName, String key, String metricName, Supplier<T> supplier) {
    return doRegisterMetric(simpleName, key, metricName, Gauge.class, supplier);
  }

  /**
   * Register a Gauge metric for the specified metricName; if it has already been registered, do nothing
   * @param simpleName the simple name of the underlying class
   * @param metricName the metric name
   * @param supplier value supplier for the Gauge
   * @return the metric just registered or previously registered one
   */
  @SuppressWarnings("unchecked")
  public <T> Gauge<T> registerGauge(String simpleName, String metricName, Supplier<T> supplier) {
    return doRegisterMetric(simpleName, null, metricName, Gauge.class, supplier);
  }

  /**
   * Unregister the metric for the specified key/metricName pair by the given value; if it has
   * never been registered, do nothing
   * @param simpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   */
  public void unregisterMetric(String simpleName, String key, String metricName) {
    validateArguments(simpleName, metricName);
    String fullMetricName = MetricRegistry.name(simpleName, key, metricName);

    try {
      _metricRegistry.remove(fullMetricName);
    } finally {
      // Always update our the index
      if (_indexedMetrics.contains(simpleName)) {
        _indexedMetrics.get(simpleName).remove(fullMetricName);
      }
    }
  }

  /**
   * Unregister the metric for the specified key/metricName pair by the given value; if it has
   * never been registered, do nothing
   * @param simpleName the simple name of the underlying class
   * @param metricName the metric name
   */
  public void unregisterMetric(String simpleName, String metricName) {
    unregisterMetric(simpleName, metricName);
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

  private Histogram registerAndGetHistogram(String fullMetricName, long windowTimeMs) {
    Histogram histogram = new Histogram(new SlidingTimeWindowReservoir(windowTimeMs, TimeUnit.MILLISECONDS));
    try {
      return _metricRegistry.register(fullMetricName, histogram);
    } catch (IllegalArgumentException e) {
      // This ideally can only happen with parallel unit tests
      LOG.warn("Failed to register for metrics {}", fullMetricName);
      LOG.warn("IllegalArgumentException during Histogram registration", e);
      return histogram;
    }
  }

  /**
   * Update the histogram (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * If the histogram does not exist, create one using {@link SlidingTimeWindowReservoir} with the specified window
   * time in ms. This can be useful for certain metrics that don't want to use the
   * default {@link com.codahale.metrics.ExponentiallyDecayingReservoir}
   * @param classSimpleName the simple name of the underlying class
   * @param key the key (i.e. topic or partition) for the metric
   * @param metricName the metric name
   * @param windowTimeMs the length of the window time in ms
   * @param value the value to update on the histogram
   */
  public void createOrUpdateSlidingWindowHistogram(String classSimpleName, String key, String metricName,
      long windowTimeMs, long value) {
    validateArguments(classSimpleName, metricName);
    String fullMetricName = MetricRegistry.name(classSimpleName, key, metricName);
    Histogram histogram = (Histogram) checkCache(classSimpleName, fullMetricName).orElse(
        registerAndGetHistogram(fullMetricName, windowTimeMs));
    histogram.update(value);
    updateCache(classSimpleName, fullMetricName, histogram);
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
