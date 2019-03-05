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
  // Map from a [class,category] to its reference counter
  private static final Map<String, AtomicInteger> REF_COUNTS = new HashMap<>();

  protected String _key;
  protected String _className;

  public BrooklinMetrics(String className, String key) {
    _className = className;
    _key = key;

    String refKey = getRefKey();
    REF_COUNTS.computeIfAbsent(refKey, k -> new AtomicInteger(0));
    REF_COUNTS.get(refKey).incrementAndGet();
  }

  private String getRefKey() {
    return getClass().getSimpleName() + _className;
  }

  /**
   * Deregister metric from metric registry
   */
  public void deregister() {
    String refKey = getRefKey();

    // Already deregistered?
    if (!REF_COUNTS.containsKey(refKey)) {
      return;
    }

    if (REF_COUNTS.get(refKey).decrementAndGet() == 0) {
      deregisterAggregates();
      REF_COUNTS.remove(refKey);
    }
  }

  protected void deregisterAggregates() {
  }
}
