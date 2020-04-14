/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link BrooklinMetricInfo}.
 */
@Test
public class TestBrooklinMetricInfo {

  @Test
  public void testBrooklinMetricInfo() {
    testBrooklinMetricInfoHelper(BrooklinMeterInfo::new, BrooklinMeterInfo::new);
    testBrooklinMetricInfoHelper(BrooklinCounterInfo::new, BrooklinCounterInfo::new);
    testBrooklinMetricInfoHelper(BrooklinGaugeInfo::new, BrooklinGaugeInfo::new);
    testBrooklinMetricInfoHelper(BrooklinHistogramInfo::new, BrooklinHistogramInfo::new);
  }

  private static <T extends BrooklinMetricInfo> void testBrooklinMetricInfoHelper(
      Function<String, T> singleArgCtor, BiFunction<String, Optional<List<String>>, T> twoArgCtor) {

    T brooklinMeterInfo1 = singleArgCtor.apply("meter1");
    T brooklinMeterInfo2 = singleArgCtor.apply("meter2");
    T brooklinMeterInfo3 = twoArgCtor.apply("meter1", Optional.of(Arrays.asList("hello", "world")));
    T brooklinMeterInfo4 = twoArgCtor.apply("meter1", Optional.of(Arrays.asList("world", "hello")));
    T brooklinMeterInfo5 = singleArgCtor.apply("meter1");

    Assert.assertNotEquals(brooklinMeterInfo1, brooklinMeterInfo2);
    Assert.assertNotEquals(brooklinMeterInfo1, brooklinMeterInfo3);
    Assert.assertEquals(brooklinMeterInfo3, brooklinMeterInfo4);
    Assert.assertEquals(brooklinMeterInfo3.hashCode(), brooklinMeterInfo4.hashCode());
    Assert.assertEquals(brooklinMeterInfo1, brooklinMeterInfo5);
    Assert.assertEquals(brooklinMeterInfo1.hashCode(), brooklinMeterInfo5.hashCode());
  }

  @Test
  public void testBrooklinDifferentMetricsComparison() {
    BrooklinMeterInfo brooklinMeterInfo1 = new BrooklinMeterInfo("meter1");
    BrooklinCounterInfo brooklinCounterInfo1 = new BrooklinCounterInfo("counter1");
    BrooklinGaugeInfo brooklinGaugeInfo1 = new BrooklinGaugeInfo("gauge1");
    BrooklinHistogramInfo brooklinHistogramInfo1 = new BrooklinHistogramInfo("histogram1");

    Assert.assertNotEquals(brooklinMeterInfo1, brooklinCounterInfo1);
    Assert.assertNotEquals(brooklinMeterInfo1, brooklinGaugeInfo1);
    Assert.assertNotEquals(brooklinMeterInfo1, brooklinHistogramInfo1);
    Assert.assertNotEquals(brooklinCounterInfo1, brooklinGaugeInfo1);
    Assert.assertNotEquals(brooklinCounterInfo1, brooklinHistogramInfo1);
    Assert.assertNotEquals(brooklinGaugeInfo1, brooklinHistogramInfo1);
  }
}