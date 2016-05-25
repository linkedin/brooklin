package com.linkedin.datastream.common;

import java.util.Map;
import java.util.SortedMap;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;


/**
 * Read-only MetricRegistry wrapper that only exposes the accessor methods of MetricRegistry.
 */
public class ReadOnlyMetricRegistry {

  private MetricRegistry _metricRegistry;

  public ReadOnlyMetricRegistry(MetricRegistry metricRegistry) {
    _metricRegistry = metricRegistry;
  }

  /**
   * Returns a map of all the gauges in the registry and their names.
   * @return all the gauges in the registry
   */
  @SuppressWarnings("rawtypes")
  public SortedMap<String, Gauge> getGauges() {
    return _metricRegistry.getGauges();
  }

  /**
   * Returns a map of all the gauges in the registry and their names which match the given filter.
   * @param filter the metric filter to match
   * @see {@link com.codahale.metrics.MetricFilter}
   * @return all the gauges in the registry that match the given filter
   */
  @SuppressWarnings("rawtypes")
  public SortedMap<String, Gauge> getGauges(MetricFilter filter) {
    return _metricRegistry.getGauges(filter);
  }

  /**
   * Returns a map of all the counters in the registry and their names.
   * @return all the counters in the registry
   */
  public SortedMap<String, Counter> getCounters() {
    return _metricRegistry.getCounters();
  }

  /**
   * Returns a map of all the counters in the registry and their names which match the given filter.
   * @param filter the metric filter to match
   * @see {@link com.codahale.metrics.MetricFilter}
   * @return all the counters in the registry that match the given filter
   */
  public SortedMap<String, Counter> getCounters(MetricFilter filter) {
    return _metricRegistry.getCounters(filter);
  }

  /**
   * Returns a map of all the histograms in the registry and their names.
   * @return all the histograms in the registry
   */
  public SortedMap<String, Histogram> getHistograms() {
    return _metricRegistry.getHistograms();
  }

  /**
   * Returns a map of all the histograms in the registry and their names which match the given filter.
   * @param filter the metric filter to match
   * @see {@link com.codahale.metrics.MetricFilter}
   * @return all the histograms in the registry that match the given filter
   */
  public SortedMap<String, Histogram> getHistograms(MetricFilter filter) {
    return _metricRegistry.getHistograms(filter);
  }

  /**
   * Returns a map of all the meters in the registry and their names.
   * @return all the meters in the registry
   */
  public SortedMap<String, Meter> getMeters() {
    return _metricRegistry.getMeters();
  }

  /**
   * Returns a map of all the meters in the registry and their names which match the given filter.
   * @param filter the metric filter to match
   * @see {@link com.codahale.metrics.MetricFilter}
   * @return all the meters in the registry that match the given filter
   */
  public SortedMap<String, Meter> getMeters(MetricFilter filter) {
    return _metricRegistry.getMeters(filter);
  }

  /**
   * Returns a map of all the timers in the registry and their names.
   * @return all the timers in the registry
   */
  public SortedMap<String, Timer> getTimers() {
    return _metricRegistry.getTimers();
  }

  /**
   * Returns a map of all the timers in the registry and their names which match the given filter.
   * @param filter the metric filter to match
   * @see {@link com.codahale.metrics.MetricFilter}
   * @return all the timers in the registry that match the given filter
   */
  public SortedMap<String, Timer> getTimers(MetricFilter filter) {
    return _metricRegistry.getTimers(filter);
  }

  /**
   * Returns a map of all the metrics in the registry and their names.
   * @return all the metrics in the registry
   */
  public Map<String, Metric> getMetrics() {
    return _metricRegistry.getMetrics();
  }
}
