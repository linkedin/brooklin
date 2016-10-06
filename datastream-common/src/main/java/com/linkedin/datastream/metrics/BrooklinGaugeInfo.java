package com.linkedin.datastream.metrics;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;


/**
 * Information about a gauge metric, which will be created dynamically.
 */
public class BrooklinGaugeInfo extends BrooklinMetricInfo {

  public static final String VALUE = "Value";

  public static final Set<String> SUPPORTED_ATTRIBUTES = Stream.of(VALUE).collect(Collectors.toSet());

  public BrooklinGaugeInfo(String nameOrRegex) {
    this(nameOrRegex, Optional.empty());
  }

  public BrooklinGaugeInfo(String nameOrRegex, Optional<List<String>> counterAttributes) {
    super(nameOrRegex, counterAttributes);
  }

}
