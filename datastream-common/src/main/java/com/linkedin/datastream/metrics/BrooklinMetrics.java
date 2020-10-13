/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

/**
 * Base class for metric categories with support for metric deregistration.
 * Underlying DynamicMetricsManager will take care of maintaining ref-counts.
 */
public abstract class BrooklinMetrics {

  protected final String _key;
  protected final String _className;

  /**
   * Constructor for BrooklinMetrics
   * @param className The class implementation that is instantiating the metrics class
   * @param key The key to use for creating the full metric names
   */
  public BrooklinMetrics(String className, String key) {
    _className = className;
    _key = key;
  }

  /**
   * Deregister metric from metric registry
   */
  public void deregister() {
    deregisterAggregates();
  }

  protected void deregisterAggregates() {
  }
}
