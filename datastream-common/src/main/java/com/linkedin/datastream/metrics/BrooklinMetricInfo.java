package com.linkedin.datastream.metrics;

import java.util.List;
import java.util.Optional;

import org.apache.commons.lang.Validate;


/**
 * Information about a metric which will be instantiated dynamically.
 */
public abstract class BrooklinMetricInfo {

  protected String _nameOrRegex;
  protected Optional<List<String>> _attributes;

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
