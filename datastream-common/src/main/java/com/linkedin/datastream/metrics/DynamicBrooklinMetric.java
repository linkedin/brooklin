package com.linkedin.datastream.metrics;

import java.util.List;
import java.util.Optional;


/**
 * Dynamic metric object for which a metric object will be created at runtime, and exact name is not known at code time.
 */
public class DynamicBrooklinMetric extends BrooklinMetric {

  private final String _metricNameRegex;

  public DynamicBrooklinMetric(String metricNameRegex, MetricType type) {
    super(type, Optional.empty());
    _metricNameRegex = metricNameRegex;
  }

  public DynamicBrooklinMetric(String metricNameRegex, MetricType type, List<String> attributes) {
    super(type, Optional.of(attributes));
    _metricNameRegex = metricNameRegex;
  }

  @Override
  public String getName() {
    return _metricNameRegex;
  }
}
