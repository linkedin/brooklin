/**
 *  Copyright 2023 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.mockito.Mockito;

import org.slf4j.Logger;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;


/**
 * Tests for {@link AssignmentTaskMapLogger}
 */
public class TestAssignmentTaskMapLogger {
  private static final DatastreamTask MOCK_TASK = mock(DatastreamTask.class);
  private static final DatastreamTask MOCK_TASK_2 = mock(DatastreamTask.class);
  private static final DatastreamTask MOCK_LARGE_TASK = Mockito.spy(new DatastreamTaskImpl());
  private static final double SIZE_LIMIT_BYTES = 1024 * 4;

  @Test
  public void testTasksLessThanSizeLimit() {
    final Logger log = mock(Logger.class);
    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(MOCK_TASK));
    taskMap.put("host2", ImmutableSet.of(MOCK_TASK, MOCK_TASK_2));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(log, SIZE_LIMIT_BYTES);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(log, times(3)).info(eq("{}={}"), contains("Live task"), anyString());

    verifyNoMoreInteractions(log);
  }

  @Test
  public void testTaskWithSizeExactlySizeLimit() {
    final Logger log = mock(Logger.class);
    Mockito.doReturn(createCustomSizeString(SIZE_LIMIT_BYTES)).when(MOCK_LARGE_TASK).toString();

    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(MOCK_TASK));
    taskMap.put("host2", ImmutableSet.of(MOCK_LARGE_TASK));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(log, SIZE_LIMIT_BYTES);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(log).info(eq("{}={}"), eq("Host=host1: Live task"), anyString());
    verify(log).info(eq("{} (part {})={}"), eq("Host=host2: Live task"), eq(1), anyString());
    verify(log).info(eq("{} (part {})={}"), eq("Host=host2: Live task"), eq(2), anyString());

    verifyNoMoreInteractions(log);
  }

  /**
   * this test expects the large task to be split even though it is slightly less than the size limit due to size buffers
   * (it's better to round up than down when determining if it's necessary to split)
   */
  @Test
  public void testTasksSlightlyLessThanSizeLimit() {
    final Logger log = mock(Logger.class);
    Mockito.doReturn(createCustomSizeString(SIZE_LIMIT_BYTES - 100)).when(MOCK_LARGE_TASK).toString();

    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(MOCK_TASK));
    taskMap.put("host2", ImmutableSet.of(MOCK_LARGE_TASK));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(log, SIZE_LIMIT_BYTES);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(log).info(eq("{}={}"), eq("Host=host1: Live task"), anyString());
    verify(log).info(eq("{} (part {})={}"), eq("Host=host2: Live task"), eq(1), anyString());
    verify(log).info(eq("{} (part {})={}"), eq("Host=host2: Live task"), eq(2), anyString());

    verifyNoMoreInteractions(log);
  }

  /**
   * this test expects the large task to be split into 3 parts even though it is 2x the size limit due to size buffers
   * (it's better to round up than down when determining if it's necessary to split)
   */
  @Test
  public void testTaskWithSize2MB() {
    final Logger log = mock(Logger.class);
    Mockito.doReturn(createCustomSizeString(SIZE_LIMIT_BYTES * 2)).when(MOCK_LARGE_TASK).toString();

    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(MOCK_TASK));
    taskMap.put("host2", ImmutableSet.of(MOCK_LARGE_TASK));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(log, SIZE_LIMIT_BYTES);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(log).info(eq("{}={}"), eq("Host=host1: Live task"), anyString());
    verify(log).info(eq("{} (part {})={}"), eq("Host=host2: Live task"), eq(1), anyString());
    verify(log).info(eq("{} (part {})={}"), eq("Host=host2: Live task"), eq(2), anyString());
    verify(log).info(eq("{} (part {})={}"), eq("Host=host2: Live task"), eq(3), anyString());

    verifyNoMoreInteractions(log);
  }

  /**
   * helper function to create string of any size
   * @param size size of string wanted
   * @return string with size number of characters
   */
  private String createCustomSizeString(double size) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      sb.append('A');
    }
    return sb.toString();
  }

}
