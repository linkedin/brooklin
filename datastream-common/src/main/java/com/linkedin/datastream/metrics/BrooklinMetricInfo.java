/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.Validate;


/**
 * Information about a metric which will be instantiated dynamically.
 */
public abstract class BrooklinMetricInfo {

  protected final String _nameOrRegex;
  protected final Optional<List<String>> _attributes;

  protected BrooklinMetricInfo(String nameOrRegex, Optional<List<String>> attributes) {
    Validate.notNull(nameOrRegex, "Metric name/regex must be non-null");
    _nameOrRegex = nameOrRegex;
    _attributes = attributes;
  }

  public Optional<List<String>> getAttributes() {
    return _attributes;
  }

  public String getNameOrRegex() {
    return _nameOrRegex;
  }
}
