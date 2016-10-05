package com.linkedin.datastream.metrics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Abstract wrapper for Codahale metric.
 */
public abstract class BrooklinMetric {

  public enum MetricType {
    METER,
    COUNTER,
    GAUGE,
    HISTOGRAM
  }

  public static final String MEAN_RATE = "MeanRate";
  public static final String ONE_MINUTE_RATE = "OneMinuteRate";
  public static final String FIVE_MINUTE_RATE = "FiveMinuteRate";
  public static final String FIFTEEN_MINUTE_RATE = "FifteenMinuteRate";

  public static final String COUNT = "Count";
  public static final String VALUE = "Value";

  public static final String MIN = "Min";
  public static final String MAX = "Max";
  public static final String MEAN = "Mean";
  public static final String STD_DEV = "StdDev";
  public static final String PERCENTILE_50 = "50thPercentile";
  public static final String PERCENTILE_75 = "75thPercentile";
  public static final String PERCENTILE_95 = "95thPercentile";
  public static final String PERCENTILE_98 = "98thPercentile";
  public static final String PERCENTILE_99 = "99thPercentile";
  public static final String PERCENTILE_999 = "999thPercentile";

  protected static final Map<MetricType, Set<String>> SUPPORTED_ATTRIBUTES = new HashMap<>();

  static {
    SUPPORTED_ATTRIBUTES.put(MetricType.METER,
        Stream.of(MEAN_RATE, ONE_MINUTE_RATE, FIVE_MINUTE_RATE, FIFTEEN_MINUTE_RATE).collect(Collectors.toSet()));
    SUPPORTED_ATTRIBUTES.put(MetricType.COUNTER, Stream.of(COUNT, VALUE).collect(Collectors.toSet()));
    SUPPORTED_ATTRIBUTES.put(MetricType.GAUGE, Stream.of(VALUE).collect(Collectors.toSet()));
    SUPPORTED_ATTRIBUTES.put(MetricType.HISTOGRAM,
        Stream.of(COUNT, MIN, MAX, MEAN, STD_DEV, PERCENTILE_50, PERCENTILE_75, PERCENTILE_95, PERCENTILE_98,
            PERCENTILE_99, PERCENTILE_999).collect(Collectors.toSet()));
  }

  protected MetricType _type;
  protected Optional<List<String>> _attributes;

  protected BrooklinMetric(MetricType type, Optional<List<String>> attributes) {
    _type = type;
    _attributes = attributes.map(
        a -> a.stream().filter(s -> SUPPORTED_ATTRIBUTES.get(_type).contains(s)).collect(Collectors.toList()));
  }

  public MetricType getType() {
    return _type;
  }

  public Optional<List<String>> getAttributes() {
    return _attributes;
  }

  public abstract String getName();

}
