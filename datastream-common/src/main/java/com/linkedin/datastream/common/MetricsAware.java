package com.linkedin.datastream.common;

import java.util.Map;

import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

/**
 * Classes that implement MetricsAware should return a map of metric name to Metric object. If there are no metrics to
 * expose, the implementation can simply return null or empty map.
 */
public interface MetricsAware {

  /**
   * Retrieve metrics
   * @return a mapping of metric name to metric
   */
  Map<String, Metric> getMetrics();

  default String buildMetricName(String metricName) {
    return MetricRegistry.name(this.getClass(), metricName);
  }

}
