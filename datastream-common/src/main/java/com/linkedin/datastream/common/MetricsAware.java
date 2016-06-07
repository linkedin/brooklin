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
   * Captures any number of dashes (-), periods, alphanumeric characters, underscores (_), or digits, followed by a period.
   * This is used to capture topic or partition specific metrics, i.e. TOPIC_NAME.numDataEvents, and is typically prefixed
   * with an exact match on class name (see {@link #getDynamicMetricPrefixRegex()})
   */
  String KEY_REGEX = "([-.\\w\\d]+)\\.";

  /**
   * Retrieve metrics
   *
   * For dynamic metrics to be captured by regular expression, since we do not have a reference to the actual Metric object,
   * simply put null value into the map.
   *
   * @return a mapping of metric name to metric
   */
  Map<String, Metric> getMetrics();

  /**
   * @return the metric name prepended with the caller's class name
   */
  default String buildMetricName(String metricName) {
    return MetricRegistry.name(this.getClass(), metricName);
  }

  /**
   * Get a regular expression for all dynamic metrics created within the class.
   *
   * For example, this regular expression should capture all topic-specific metrics emitted by KafkaTransportProvider
   * with the given format: com.linkedin.datastream.kafka.KafkaTransportProvider.DYNAMIC_TOPIC_NAME.numDataEvents
   *
   * This regular expression purposely does not capture non topic-specific metrics. For example, non topic-specific
   * metric emitted by KafkaTransportProvider with the format com.linkedin.datastream.kafka.KafkaTransportProvider.numDataEvents
   * will not be captured.
   *
   * @return the regular expression to capture all dynamic metrics that will be created within the class
   */
  default String getDynamicMetricPrefixRegex() {
    return this.getClass().getName() + KEY_REGEX;
  }
}
