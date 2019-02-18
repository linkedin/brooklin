/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Base class for metric categories with support for metric deregistration.
 * It uses reference counting for deregistering aggregate metrics which can
 * only happen when all keyed metrics of the same name have been deregistered.
 */
public abstract class BrooklinMetrics {
  protected String _key;
  protected String _className;

  // Map from a [class,category] to its reference counter
  private static Map<String, AtomicInteger> _refCounts = new HashMap<>();

  private String getRefKey() {
    return getClass().getSimpleName() + _className;
  }

  public BrooklinMetrics(String className, String key) {
    _className = className;
    _key = key;

    String refKey = getRefKey();
    _refCounts.computeIfAbsent(refKey, k -> new AtomicInteger(0));
    _refCounts.get(refKey).incrementAndGet();
  }

  /**
   * Deregister metric from metric registry
   */
  public void deregister() {
    String refKey = getRefKey();

    // Already deregistered?
    if (!_refCounts.containsKey(refKey)) {
      return;
    }

    if (_refCounts.get(refKey).decrementAndGet() == 0) {
      deregisterAggregates();
      _refCounts.remove(refKey);
    }
  }

  protected void deregisterAggregates() {
  }
}
