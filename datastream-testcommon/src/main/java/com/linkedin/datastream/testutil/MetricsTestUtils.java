/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.testng.Assert;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import com.google.common.collect.ImmutableMap;

import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;


/**
 * Test utilities for verifying metrics
 * @see MetricsAware
 */
public class MetricsTestUtils {
  // Mapping of BrooklinMetricInfo sub-types -> com.codahale.metrics types
  private static final Map<Class<? extends BrooklinMetricInfo>, Class<? extends Metric>> METRICS_TYPE_MAPPING = ImmutableMap.of(
      BrooklinCounterInfo.class, Counter.class,
      BrooklinGaugeInfo.class, Gauge.class,
      BrooklinHistogramInfo.class, Histogram.class,
      BrooklinMeterInfo.class, Meter.class);

  private MetricsTestUtils() {
  }

  /**
   * @see #verifyMetrics(MetricsAware, DynamicMetricsManager, Predicate)
   *
   * This verification only covers {@link Metric} and {@link BrooklinMetricInfo} objects whose
   * names start with the provided {@code metricsAware}'s simple class name.
   */
  public static void verifyMetrics(MetricsAware metricsAware, DynamicMetricsManager metricsManager) {
    verifyMetrics(metricsAware, metricsManager, s -> s.startsWith(metricsAware.getClass().getSimpleName()));
  }

  /**
   * Verifies consistency between:
   * <ol>
   *   <li>{@link BrooklinMetricInfo} objects returned by the specified {@code metricsAware}
   *   (specifically {@link MetricsAware#getMetricInfos()}) after filtering them using {@code metricFilter}</li>
   *   <li>{@link Metric} objects registered with the specified {@code metricsManager} after filtering them
   *   using {@code metricFilter}</li>
   * </ol>
   *
   * In particular, it verifies that:
   * <ul>
   *   <li>All {@link BrooklinMetricInfo} are distinct</li>
   *   <li>Every {@link Metric} object has exactly one {@link BrooklinMetricInfo} object whose
   *   {@link BrooklinMetricInfo#getNameOrRegex()} matches the metric's name</li>
   *   <li>Every {@link Metric} is matched with a {@link BrooklinMetricInfo} object whose concrete
   *   type matches the metric's type according to {@link MetricsTestUtils#METRICS_TYPE_MAPPING}</li>
   * </ul>
   */
  public static void verifyMetrics(MetricsAware metricsAware, DynamicMetricsManager metricsManager,
      Predicate<String> metricFilter) {

    List<BrooklinMetricInfo> metricInfos = metricsAware.getMetricInfos().stream()
        .filter(metricInfo -> metricFilter.test(metricInfo.getNameOrRegex()))
        .collect(Collectors.toList());

    // Assert that all metricInfos are distinct
    Assert.assertEquals(metricInfos.size(), metricInfos.stream().distinct().count(),
        "The supplied metricsAware returned duplicate BrooklinMetricInfo objects");

    Map<String, BrooklinMetricInfo> metricInfoByName = new HashMap<>();
    List<BrooklinMetricInfo> regexMetricInfos = new ArrayList<>();
    classifyMetricInfos(metricInfos, metricInfoByName, regexMetricInfos);

    Collection<Map.Entry<String, Metric>> metricEntries = metricsManager.getMetricRegistry().getMetrics().entrySet().stream()
        .filter(metricEntry -> metricFilter.test(metricEntry.getKey()))
        .collect(Collectors.toList());

    // Assert that every metric has exactly one metricInfo whose nameOrRegex matches the metric's name
    for (Map.Entry<String, Metric> metricEntry: metricEntries) {
      int count = getMatchingMetricInfoCount(metricEntry, metricInfoByName, regexMetricInfos);
      Assert.assertEquals(count, 1,
          String.format("Metric %s must match exactly one BrooklinMetricInfo but it matched %d",
              metricEntry.getKey(), count));
    }
  }

  private static void classifyMetricInfos(Collection<BrooklinMetricInfo> metricInfos,
      Map<String, BrooklinMetricInfo> metricInfoByName, List<BrooklinMetricInfo> regexMetricInfos) {
    for (BrooklinMetricInfo metricInfo : metricInfos) {
      String nameOrRegex = metricInfo.getNameOrRegex();
      if (nameOrRegex.contains(MetricsAware.KEY_REGEX)) {
        regexMetricInfos.add(metricInfo);
      } else {
        metricInfoByName.put(nameOrRegex, metricInfo);
      }
    }
  }

  private static int getMatchingMetricInfoCount(Map.Entry<String, Metric> metricEntry,
      Map<String, BrooklinMetricInfo> metricInfoByName, List<BrooklinMetricInfo> regexMetricInfos) {
    int count = 0;

    String metricName = metricEntry.getKey();
    if (metricInfoByName.containsKey(metricName)) {
      BrooklinMetricInfo metricInfo = metricInfoByName.get(metricName);
      assertTypeCompatibility(metricInfo, metricEntry);
      ++count;
    }

    for (BrooklinMetricInfo metricInfo : regexMetricInfos) {
      if (metricName.matches(metricInfo.getNameOrRegex())) {
        assertTypeCompatibility(metricInfo, metricEntry);
        ++count;
      }
    }

    return count;
  }

  private static void assertTypeCompatibility(BrooklinMetricInfo metricInfo, Map.Entry<String, Metric> metricEntry) {
    String metricName = metricEntry.getKey();
    Metric metric = metricEntry.getValue();

    Class<? extends BrooklinMetricInfo> metricInfoClass = metricInfo.getClass();
    Class<? extends Metric> expectedMetricClass = METRICS_TYPE_MAPPING.get(metricInfoClass);
    Class<? extends Metric> actualMetricClass = metric.getClass();

    // Assert that every metric is matched with a metricInfo whose concrete type matches the expected metric's type
    Assert.assertTrue(expectedMetricClass.isAssignableFrom(actualMetricClass), String.format(
        "Identified a mismatch between %s and metric %s. Expected metric type to be %s but found %s",
        metricInfo, metricName, expectedMetricClass, actualMetricClass));
  }
}
