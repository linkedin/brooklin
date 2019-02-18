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
 * Information about a meter metric, which will be created dynamically.
 */
public class BrooklinMeterInfo extends BrooklinMetricInfo {

  public static final String COUNT = "Count";
  public static final String MEAN_RATE = "MeanRate";
  public static final String ONE_MINUTE_RATE = "OneMinuteRate";

  public static final Set<String> SUPPORTED_ATTRIBUTES =
      Stream.of(COUNT, MEAN_RATE, ONE_MINUTE_RATE).collect(Collectors.toSet());

  public BrooklinMeterInfo(String nameOrRegex) {
    this(nameOrRegex, Optional.empty());
  }

  public BrooklinMeterInfo(String nameOrRegex, Optional<List<String>> meterAttributes) {
    super(nameOrRegex, meterAttributes);
  }

}
