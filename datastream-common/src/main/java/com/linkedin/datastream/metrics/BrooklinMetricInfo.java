/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

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

  private boolean equalAttributes(Optional<List<String>> attributes1, Optional<List<String>> attributes2) {
    if (!(attributes1.isPresent() == attributes2.isPresent())) {
      return false;
    }
    if (!attributes1.isPresent()) {
      return true;
    }

    Set<String> attributeSet1 = new HashSet<>();
    attributes1.ifPresent(attributeSet1::addAll);

    Set<String> attributeSet2 = new HashSet<>();
    attributes2.ifPresent(attributeSet2::addAll);

    return attributeSet1.equals(attributeSet2);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrooklinMetricInfo that = (BrooklinMetricInfo) o;
    return _nameOrRegex.equals(that._nameOrRegex) && equalAttributes(_attributes, that._attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_nameOrRegex, _attributes);
  }
}
