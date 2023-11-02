package com.linkedin.datastream.server;

import com.google.common.collect.ImmutableSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


/**
 * Tests for {@link AssignmentTaskMapLogger}
 */
public class TestAssignmentTaskMapLogger {

  private static final Logger LOG = mock(Logger.class);
  private static final DatastreamTask mockTask = mock(DatastreamTask.class);
  private static final DatastreamTask mockTask2 = mock(DatastreamTask.class);
  private static final DatastreamTask largeTaskMock = Mockito.spy(new DatastreamTaskImpl());

  @Test
  public void testTasksLessThan1MB() {
    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(mockTask));
    taskMap.put("host2", ImmutableSet.of(mockTask, mockTask2));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(LOG);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(LOG, times(3)).info(eq("Host={}: Live task={}"), contains("host"), anyString());

    verifyNoMoreInteractions(LOG);
  }

  @Test
  public void testTaskWithSize1MB() {
    Mockito.doReturn(createCustomSizeString(1024 * 1024)).when(largeTaskMock).toString();

    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(mockTask));
    taskMap.put("host2", ImmutableSet.of(largeTaskMock));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(LOG);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(LOG, times(1)).info(eq("Host={}: Live task={}"), eq("host1"), anyString());
    verify(LOG, times(1)).info(eq("Host={}: Live task (part {})={}"), eq("host2"), eq(1), anyString());
    verify(LOG, times(1)).info(eq("Host={}: Live task (part {})={}"), eq("host2"), eq(2), anyString());

    verifyNoMoreInteractions(LOG);
  }

  /**
   * this test expects the large task to be split even though it is slightly less than 1 MB due to size buffers
   * (it's better to round up than down when determining if it's necessary to split)
   */
  @Test
  public void testTasksSlightlyLessThan1MB() {
    Mockito.doReturn(createCustomSizeString(1024 * 1023)).when(largeTaskMock).toString();

    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(mockTask));
    taskMap.put("host2", ImmutableSet.of(largeTaskMock));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(LOG);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(LOG, times(1)).info(eq("Host={}: Live task={}"), eq("host1"), anyString());
    verify(LOG, times(1)).info(eq("Host={}: Live task (part {})={}"), eq("host2"), eq(1), anyString());
    verify(LOG, times(1)).info(eq("Host={}: Live task (part {})={}"), eq("host2"), eq(2), anyString());

    verifyNoMoreInteractions(LOG);
  }

  /**
   * this test expects the large task to be split into 3 parts even though it is 2MB due to size buffers
   * (it's better to round up than down when determining if it's necessary to split)
   */
  @Test
  public void testTaskWithSize2MB() {
    Mockito.doReturn(createCustomSizeString(1024 * 1024 * 2)).when(largeTaskMock).toString();

    Map<String, Set<DatastreamTask>> taskMap = new HashMap<>();
    taskMap.put("host1", ImmutableSet.of(mockTask));
    taskMap.put("host2", ImmutableSet.of(largeTaskMock));

    AssignmentTaskMapLogger logger = new AssignmentTaskMapLogger(LOG);
    long startTime = System.currentTimeMillis();
    logger.logAssignment(taskMap);
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;
    System.out.println("Log assignment duration: " + duration + " ms");

    verify(LOG, times(1)).info(eq("Host={}: Live task={}"), eq("host1"), anyString());
    verify(LOG, times(1)).info(eq("Host={}: Live task (part {})={}"), eq("host2"), eq(1), anyString());
    verify(LOG, times(1)).info(eq("Host={}: Live task (part {})={}"), eq("host2"), eq(2), anyString());
    verify(LOG, times(1)).info(eq("Host={}: Live task (part {})={}"), eq("host2"), eq(3), anyString());

    verifyNoMoreInteractions(LOG);
  }

  /**
   * helper function to create string of any size
   * @param size size of string wanted
   * @return string with size number of characters
   */
  private String createCustomSizeString(int size) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < size; i++) {
      sb.append('A');
    }
    return sb.toString();
  }

}
