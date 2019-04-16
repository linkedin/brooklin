/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;


/**
 * This class traps threads with unhandled exceptions and
 * reports abnormal terminations and reports them as metric.
 * However, it doesn't attempt to recover the condition.
 */
public class ThreadTerminationMonitor {

  private static final String ABNORMAL_TERMINATIONS = "abnormalTerminations";

  private static final String MODULE = ThreadTerminationMonitor.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(ThreadTerminationMonitor.class);

  private static final Thread.UncaughtExceptionHandler OLD_HANDLER;

  static {
    // Replace the default uncaught exception handler
    OLD_HANDLER = Thread.getDefaultUncaughtExceptionHandler();
    Thread.setDefaultUncaughtExceptionHandler((Thread t, Throwable e) -> {
      LOG.error(String.format("Thread %s terminated abnormally", t.getName()), e);
      DynamicMetricsManager.getInstance().createOrUpdateMeter(MODULE, ABNORMAL_TERMINATIONS, 1);
      // Resume the old behavior
      if (OLD_HANDLER != null) {
        OLD_HANDLER.uncaughtException(t, e);
      }
    });
  }

  /**
   * Creates metrics info relevant to ThreadTerminatorMonitor
   */
  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.add(new BrooklinMeterInfo(
        MetricRegistry.name(ThreadTerminationMonitor.class.getSimpleName(), ABNORMAL_TERMINATIONS)));
    return Collections.unmodifiableList(metrics);
  }
}
