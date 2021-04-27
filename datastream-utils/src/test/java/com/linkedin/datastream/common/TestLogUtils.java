/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  @Test
  public void testLogSummarizedTopicPartitionsMapping() throws Exception {
    List<String> input = Arrays.asList("t-1", "t-3", "t-6", "t-4", "t-7", "t-12", "t-9", "t-10", "t-8");
    Assert.assertEquals(LogUtils.logSummarizedTopicPartitionsMapping(input), "t:[1, 3-4, 6-10, 12]");

    input = Arrays.asList("t-3", "t-1", "t-4", "t-6", "t-7", "t-8", "t-9", "t-9", "t-9", "t-10", "t-11", "t-12", "t1-9", "t1-10", "t1-11", "t1-12");
    Assert.assertEquals(
        getTokenizedTopicPartitionMappings(LogUtils.logSummarizedTopicPartitionsMapping(input)),
        getTokenizedTopicPartitionMappings("t:[1, 3-4, 6-12], t1:[9-12]"));

    // don't modify the original input list
    Assert.assertEquals(input.get(0), "t-3");

    input = Arrays.asList("t-1", "t-5", "t-6", "t-4", "t-2", "t-3", "t1-1", "t1-5", "t1-6", "t1-4", "t1-2", "t1-3");
    Assert.assertEquals(
        getTokenizedTopicPartitionMappings(LogUtils.logSummarizedTopicPartitionsMapping(input)),
        getTokenizedTopicPartitionMappings("t:[1-6], t1:[1-6]"));

    input = Arrays.asList("t-a-b-1", "t-a-b-2", "t-a--b-7", "t-a--b-8");
    Assert.assertEquals(
        getTokenizedTopicPartitionMappings(LogUtils.logSummarizedTopicPartitionsMapping(input)),
        getTokenizedTopicPartitionMappings("t-a--b:[7-8], t-a-b:[1-2]"));

    input = Arrays.asList("t-1", "t-5", "t-6", "t-4", "t-2", "t-");
    Assert.assertEquals(LogUtils.logSummarizedTopicPartitionsMapping(input), "t-1,t-5,t-6,t-4,t-2,t-");

    Assert.assertEquals(LogUtils.logSummarizedTopicPartitionsMapping(null), "[]");
    Assert.assertEquals(LogUtils.logSummarizedTopicPartitionsMapping(Collections.emptyList()), "[]");
  }

  Set<String> getTokenizedTopicPartitionMappings(String topicPartitionMapping) {
    return Stream.of(topicPartitionMapping.split(", ")).collect(Collectors.toSet());
  }
}
