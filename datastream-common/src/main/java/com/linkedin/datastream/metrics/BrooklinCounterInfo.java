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
 * Information about a counter metric, which will be created dynamically.
 */
public class BrooklinCounterInfo extends BrooklinMetricInfo {

  public static final String COUNT = "Count";
  public static final String VALUE = "Value";

  public static final Set<String> SUPPORTED_ATTRIBUTES = Stream.of(COUNT, VALUE).collect(Collectors.toSet());

  public BrooklinCounterInfo(String nameOrRegex) {
    this(nameOrRegex, Optional.empty());
  }

  public BrooklinCounterInfo(String nameOrRegex, Optional<List<String>> counterAttributes) {
    super(nameOrRegex, counterAttributes);
  }

}
