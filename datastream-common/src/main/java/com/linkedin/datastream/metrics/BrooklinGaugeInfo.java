/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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

  /**
   * Construct an instance of BrooklinGaugeInfo
   * @param nameOrRegex Gauge name or regex
   */
  public BrooklinGaugeInfo(String nameOrRegex) {
    this(nameOrRegex, Optional.empty());
  }

  /**
   * Construct an instance of BrooklinGaugeInfo
   * @param nameOrRegex Gauge name or regex
   * @param gaugeAttributes Gauge attributes
   */
  public BrooklinGaugeInfo(String nameOrRegex, Optional<List<String>> gaugeAttributes) {
    super(nameOrRegex, gaugeAttributes);
  }
}
