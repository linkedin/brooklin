/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import javax.annotation.Nullable;

import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowArrayReservoir;
import com.codahale.metrics.Timer;


/**
 * Manages dynamic metrics and supports creating/updating metrics on the fly.
 */
public class DynamicMetricsManager {
  static final String NO_KEY_PLACEHOLDER = "NO_KEY";
  private static final Logger LOG = LoggerFactory.getLogger(DynamicMetricsManager.class);
  private static DynamicMetricsManager _instance = null;
  // Metrics indexed by simple class name, key (if exists), and metric name
  // Simple class name -> key -> metric name -> Metric object
  //   For example: LiKafkaTransportProvider.topicName.eventsProcessedRate
  //   would be stored in the cache as: LiKafkaTransportProvider -> topicName -> eventsProcessedRate -> Meter object
  // If key is not used, in the case of class level metrics, "NO_KEY" will be used as a placeholder in the cache
  //   For example: LiKafkaTransportProvider.errorRate
  //   would be stored in the cache as: LiKafkaTransportProvider -> NO_KEY -> errorRate -> Meter object
  // The map helps reducing contention on the internal map of MetricRegistry. The multiple levels of indexing by each
  // part of the full metric name helps to avoid too many String concatenations, which impacts performance.
  // This is created solely for the createOrUpdate APIs, not by registerMetric because the former can be called
  // repeatedly to update the metric whereas the latter is typically only called once per metric during initialization.
  private final ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>>> _indexedMetrics;

  // Map to maintain the ref count for the registered metric. When the ref count becomes zero or not the key is not present,
  // the metric can be deregistered.
  private final ConcurrentHashMap<String, Integer> _registeredMetricRefCount;
  private MetricRegistry _metricRegistry;

  private DynamicMetricsManager(MetricRegistry metricRegistry) {
    _metricRegistry = metricRegistry;
    _indexedMetrics = new ConcurrentHashMap<>();
    _registeredMetricRefCount = new ConcurrentHashMap<>();
  }

  /**
   * Instantiate the singleton of DynamicMetricsManager; This is a no-op if it already exists.
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
   */
  public static DynamicMetricsManager createInstance(MetricRegistry metricRegistry, @Nullable String testName) {
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

  /**
   * Get the DynamicMetricsManager instance
   */
  public static DynamicMetricsManager getInstance() {
    if (_instance == null) {
      throw new IllegalStateException("DynamicMetricsManager has not yet been instantiated.");
    }
    return _instance;
  }

  Optional<Metric> checkCache(String simpleClassName, String key, String metric) {
    String keyIndex = key == null ? NO_KEY_PLACEHOLDER : key;
    return Optional.of(getClassMetrics(simpleClassName))
        .map(classMetrics -> classMetrics.get(keyIndex))
        .map(keyMetrics -> keyMetrics.get(metric));
  }

  private void updateCache(String simpleClassName, String key, String metricName, Metric metric) {
    String keyIndex = key == null ? NO_KEY_PLACEHOLDER : key;
    ConcurrentHashMap<String, Metric> keyMetrics =
        getClassMetrics(simpleClassName).computeIfAbsent(keyIndex, k -> new ConcurrentHashMap<>());
    keyMetrics.put(metricName, metric);
  }

  private ConcurrentHashMap<String, ConcurrentHashMap<String, Metric>> getClassMetrics(String simpleClassName) {
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
   */
  @SuppressWarnings("unchecked")
  private <T extends Metric, V> T doRegisterMetric(String simpleName, String key, String metricName,
      Class<T> metricClass, Supplier<V> supplier) {
    validateArguments(simpleName, metricName);
    Validate.notNull(metricClass, "metric class argument is null.");

    String fullMetricName = MetricRegistry.name(simpleName, key, metricName);

    Metric metric = getMetric(fullMetricName, metricClass);
    _registeredMetricRefCount.compute(fullMetricName, (localKey, val) -> (val == null) ? 1 : val + 1);
    if (metric instanceof ResettableGauge) {
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
    _registeredMetricRefCount.computeIfPresent(fullMetricName, (localKey, val) -> (val == 1) ? null : val - 1);
    try {
      if (!_registeredMetricRefCount.containsKey(fullMetricName)) {
        _metricRegistry.remove(fullMetricName);
      }
    } finally {
      // Always update the index
      if (_indexedMetrics.containsKey(simpleName)) {
        String keyIndex = key == null ? NO_KEY_PLACEHOLDER : key;
        if (_indexedMetrics.get(simpleName).containsKey(keyIndex)) {
          _indexedMetrics.get(simpleName).get(keyIndex).remove(metricName);
        }
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
    unregisterMetric(simpleName, null, metricName);
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

    // create and register the metric if it does not exist
    Counter counter = (Counter) checkCache(classSimpleName, key, metricName).orElseGet(() -> {
      Counter newCounter = _metricRegistry.counter(MetricRegistry.name(classSimpleName, key, metricName));
      updateCache(classSimpleName, key, metricName, newCounter);
      return newCounter;
    });
    counter.inc(value);
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

    // create and register the metric if it does not exist
    Meter meter = (Meter) checkCache(classSimpleName, key, metricName).orElseGet(() -> {
      Meter newMeter = _metricRegistry.meter(MetricRegistry.name(classSimpleName, key, metricName));
      updateCache(classSimpleName, key, metricName, newMeter);
      return newMeter;
    });
    meter.mark(value);
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

  // This function should only be called after "checkCache". So using "synchronized" shouldn't be a problem. The race
  // will only happen briefly after the process starts and before the cache is populated.
  private synchronized Histogram registerAndGetSlidingWindowHistogram(String fullMetricName, long windowTimeMs) {
    Histogram histogram = new Histogram(new SlidingTimeWindowArrayReservoir(windowTimeMs, TimeUnit.MILLISECONDS));
    try {
      return _metricRegistry.register(fullMetricName, histogram);
    } catch (IllegalArgumentException e) {
      // This could happen when multiple threads call createOrUpdateSlidingWindowHistogram simultaneously
      // In that case the line below will just return the one that got registered first.
      return _metricRegistry.histogram(fullMetricName);
    }
  }

  /**
   * Update the histogram (or creates it if it does not exist) for the specified key/metricName pair by the given value.
   * If the histogram does not exist, create one using {@link SlidingTimeWindowArrayReservoir} with the specified window
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
    Histogram histogram = (Histogram) checkCache(classSimpleName, key, metricName).orElseGet(() -> {
      Histogram newHistogram =
          registerAndGetSlidingWindowHistogram(MetricRegistry.name(classSimpleName, key, metricName), windowTimeMs);
      updateCache(classSimpleName, key, metricName, newHistogram);
      return newHistogram;
    });
    histogram.update(value);
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
    // create and register the metric if it does not exist
    Histogram histogram = (Histogram) checkCache(classSimpleName, key, metricName).orElseGet(() -> {
      Histogram newHistogram = _metricRegistry.histogram(MetricRegistry.name(classSimpleName, key, metricName));
      updateCache(classSimpleName, key, metricName, newHistogram);
      return newHistogram;
    });
    histogram.update(value);
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
   */
  @SuppressWarnings("unchecked")
  public <T extends Metric> T getMetric(String name) {
    return (T) _metricRegistry.getMetrics().getOrDefault(name, null);
  }

  private void validateArguments(String classSimpleName, String metricName) {
    Validate.notNull(classSimpleName, "classSimpleName argument is null.");
    Validate.notNull(metricName, "metricName argument is null.");
  }

  /**
   * Get metricRegistry object, it allows other module like Kafka client to wire in the same metricRegistry
   */
  public MetricRegistry getMetricRegistry() {
    return _metricRegistry;
  }
}
