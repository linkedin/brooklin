package com.linkedin.datastream.metrics;

import java.util.List;
import java.util.Optional;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;


/**
 * Wrapper for Codahale metric for which name and type are known at code time.
 */
public class StaticBrooklinMetric extends BrooklinMetric {

  private final String _name;
  private final Metric _metric;

  public StaticBrooklinMetric(String name, Meter metric) {
    this(name, metric, null);
  }

  public StaticBrooklinMetric(String name, Counter metric) {
    this(name, metric, null);
  }

  @SuppressWarnings("rawtypes")
  public StaticBrooklinMetric(String name, Gauge metric) {
    this(name, metric, null);
  }

  public StaticBrooklinMetric(String name, Histogram metric) {
    this(name, metric, null);
  }

  public StaticBrooklinMetric(String name, Meter metric, List<String> attributes) {
    super(MetricType.METER, Optional.ofNullable(attributes));
    _name = name;
    _metric = metric;
  }

  public StaticBrooklinMetric(String name, Counter metric, List<String> attributes) {
    super(MetricType.COUNTER, Optional.ofNullable(attributes));
    _name = name;
    _metric = metric;
  }

  @SuppressWarnings("rawtypes")
  public StaticBrooklinMetric(String name, Gauge metric, List<String> attributes) {
    super(MetricType.GAUGE, Optional.ofNullable(attributes));
    _name = name;
    _metric = metric;
  }

  public StaticBrooklinMetric(String name, Histogram metric, List<String> attributes) {
    super(MetricType.HISTOGRAM, Optional.ofNullable(attributes));
    _name = name;
    _metric = metric;
  }

  /**
   * @return the name of the metric
   */
  @Override
  public String getName() {
    return _name;
  }

  /**
   * @return the metric
   */
  public Metric getMetric() {
    return _metric;
  }

}
