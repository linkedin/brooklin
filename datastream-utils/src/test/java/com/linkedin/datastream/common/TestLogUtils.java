/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.Arrays;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link LogUtils}
 */
public class TestLogUtils {
  @Test
  public void testLogNumberArrayInRange() throws Exception {
    List<Integer> input = Arrays.asList(1, 3, 6, 4, 7, 12, 9, 10, 8);
    Assert.assertEquals(LogUtils.logNumberArrayInRange(input), "[1, 3-4, 6-10, 12]");

    input = Arrays.asList(3, 1, 4, 6, 7, 8, 9, 9, 9, 10, 11, 12);
    Assert.assertEquals(LogUtils.logNumberArrayInRange(input), "[1, 3-4, 6-12]");
    // don't modify the original input list
    Assert.assertEquals(input.get(0).intValue(), 3);

    input = Arrays.asList(1, 5, 6, 4, 2, 3);
    Assert.assertEquals(LogUtils.logNumberArrayInRange(input), "[1-6]");

    Assert.assertEquals(LogUtils.logNumberArrayInRange(null), "[]");
  }
}
