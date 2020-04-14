/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.metrics;

import java.util.Arrays;
import java.util.Optional;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link BrooklinMetrics}.
 */
public class TestMetrics {
  @Test
  public void testBrooklinMeterInfoComparison() {
    BrooklinMeterInfo brooklinMeterInfo1 = new BrooklinMeterInfo("meter1");
    BrooklinMeterInfo brooklinMeterInfo2 = new BrooklinMeterInfo("meter2");
    BrooklinMeterInfo brooklinMeterInfo3 = new BrooklinMeterInfo("meter1",
        Optional.of(Arrays.asList("hello", "world")));
    BrooklinMeterInfo brooklinMeterInfo4 = new BrooklinMeterInfo("meter1",
        Optional.of(Arrays.asList("world", "hello")));
    BrooklinMeterInfo brooklinMeterInfo5 = new BrooklinMeterInfo("meter1");
    Assert.assertNotEquals(brooklinMeterInfo1, brooklinMeterInfo2);
    Assert.assertNotEquals(brooklinMeterInfo1, brooklinMeterInfo3);
    Assert.assertEquals(brooklinMeterInfo3, brooklinMeterInfo4);
    Assert.assertEquals(brooklinMeterInfo1, brooklinMeterInfo5);
  }

  @Test
  public void testBrooklinCounterInfoMetricsComparison() {
    BrooklinCounterInfo brooklinCounterInfo1 = new BrooklinCounterInfo("counter1");
    BrooklinCounterInfo brooklinCounterInfo2 = new BrooklinCounterInfo("counter2");
    BrooklinCounterInfo brooklinCounterInfo3 = new BrooklinCounterInfo("counter1",
        Optional.of(Arrays.asList("hello", "world")));
    BrooklinCounterInfo brooklinCounterInfo4 = new BrooklinCounterInfo("counter1",
        Optional.of(Arrays.asList("world", "hello")));
    BrooklinCounterInfo brooklinCounterInfo5 = new BrooklinCounterInfo("counter1");
    Assert.assertNotEquals(brooklinCounterInfo1, brooklinCounterInfo2);
    Assert.assertNotEquals(brooklinCounterInfo1, brooklinCounterInfo3);
    Assert.assertEquals(brooklinCounterInfo3, brooklinCounterInfo4);
    Assert.assertEquals(brooklinCounterInfo1, brooklinCounterInfo5);
  }

  @Test
  public void testBrooklinGaugeInfoComparison() {
    BrooklinGaugeInfo brooklinGaugeInfo1 = new BrooklinGaugeInfo("gauge1");
    BrooklinGaugeInfo brooklinGaugeInfo2 = new BrooklinGaugeInfo("gauge2");
    BrooklinGaugeInfo brooklinGaugeInfo3 = new BrooklinGaugeInfo("gauge1",
        Optional.of(Arrays.asList("hello", "world")));
    BrooklinGaugeInfo brooklinGaugeInfo4 = new BrooklinGaugeInfo("gauge1",
        Optional.of(Arrays.asList("world", "hello")));
    BrooklinGaugeInfo brooklinGaugeInfo5 = new BrooklinGaugeInfo("gauge1");
    Assert.assertNotEquals(brooklinGaugeInfo1, brooklinGaugeInfo2);
    Assert.assertNotEquals(brooklinGaugeInfo1, brooklinGaugeInfo3);
    Assert.assertEquals(brooklinGaugeInfo3, brooklinGaugeInfo4);
    Assert.assertEquals(brooklinGaugeInfo1, brooklinGaugeInfo5);
  }

  @Test
  public void testBrooklinHistogramInfoComparison() {
    BrooklinHistogramInfo brooklinHistogramInfo1 = new BrooklinHistogramInfo("histogram1");
    BrooklinHistogramInfo brooklinHistogramInfo2 = new BrooklinHistogramInfo("histogram2");
    BrooklinHistogramInfo brooklinHistogramInfo3 = new BrooklinHistogramInfo("histogram1",
        Optional.of(Arrays.asList("hello", "world")));
    BrooklinHistogramInfo brooklinHistogramInfo4 = new BrooklinHistogramInfo("histogram1",
        Optional.of(Arrays.asList("world", "hello")));
    BrooklinHistogramInfo brooklinHistogramInfo5 = new BrooklinHistogramInfo("histogram1");
    Assert.assertNotEquals(brooklinHistogramInfo1, brooklinHistogramInfo2);
    Assert.assertNotEquals(brooklinHistogramInfo1, brooklinHistogramInfo3);
    Assert.assertEquals(brooklinHistogramInfo3, brooklinHistogramInfo4);
    Assert.assertEquals(brooklinHistogramInfo1, brooklinHistogramInfo5);
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
