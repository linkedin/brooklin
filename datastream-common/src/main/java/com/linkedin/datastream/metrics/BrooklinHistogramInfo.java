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
 * Information about a histogram metric, which will be created dynamically.
 */
public class BrooklinHistogramInfo extends BrooklinMetricInfo {

  public static final String COUNT = "Count";
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

  public static final Set<String> SUPPORTED_ATTRIBUTES =
      Stream.of(COUNT, MIN, MAX, MEAN, STD_DEV, PERCENTILE_50, PERCENTILE_75, PERCENTILE_95, PERCENTILE_98, PERCENTILE_99,
          PERCENTILE_999).collect(Collectors.toSet());

  /**
   * Construct an instance of BrooklinHistogramInfo
   * @param nameOrRegex Name or regex of the histogram
   */
  public BrooklinHistogramInfo(String nameOrRegex) {
    this(nameOrRegex, Optional.empty());
  }

  /**
   * Construct an instance of BrooklinHistogramInfo
   * @param nameOrRegex Name or regex of the histogram
   * @param histogramAttributes Attributes of the histogram. It is optional.
   */
  public BrooklinHistogramInfo(String nameOrRegex, Optional<List<String>> histogramAttributes) {
    super(nameOrRegex, histogramAttributes);
  }
}
