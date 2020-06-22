/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


/**
 * Tests for {@link DatastreamTask}
 */
public class TestDatastreamTask {

  @Test
  public void testAcquireWithDependencies() throws Exception {
    Datastream stream = DatastreamTestUtils.createDatastream("dummy", "dummy", "dummy");
    stream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(stream));

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(stream));
    ZkAdapter mockZkAdapter = mock(ZkAdapter.class);
    task.setZkAdapter(mockZkAdapter);

    task.addDependency("task0");
    task.acquire(Duration.ofMillis(60));
    verify(mockZkAdapter, atLeastOnce()).waitForDependencies(any(DatastreamTaskImpl.class), any(Duration.class));
  }

  @Test(expectedExceptions = DatastreamTransientException.class)
  public void testCreateNewTaskFromUnlockedTask() throws Exception {
    Datastream stream = DatastreamTestUtils.createDatastream("dummy", "dummy", "dummy");
    stream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(stream));

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(stream));
    task.setPartitionsV2(ImmutableList.of("partition1"));
    task.addDependency("task0");
    ZkAdapter mockZkAdapter = mock(ZkAdapter.class);
    task.setZkAdapter(mockZkAdapter);
    when(mockZkAdapter.checkIsTaskLocked(anyString(), anyString())).thenReturn(false);
    DatastreamTaskImpl task2 = new DatastreamTaskImpl(task, new ArrayList<>());
  }

  @Test
  public void testCreateNewTaskFromLockedTask() throws Exception {
    Datastream stream = DatastreamTestUtils.createDatastream("dummy", "dummy", "dummy");
    stream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(stream));

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(stream));
    task.addDependency("task0");
    ZkAdapter mockZkAdapter = mock(ZkAdapter.class);
    task.setZkAdapter(mockZkAdapter);
    when(mockZkAdapter.checkIsTaskLocked(anyString(), anyString())).thenReturn(true);
    DatastreamTaskImpl task2 = new DatastreamTaskImpl(task, new ArrayList<>());
    Assert.assertEquals(new HashSet<>(task2.getDependencies()), ImmutableSet.of(task.getDatastreamTaskName()));
  }

  @Test
  public void testDatastreamTaskJson() throws Exception {
    Datastream stream = DatastreamTestUtils.createDatastream("dummy", "dummy", "dummy");
    stream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(stream));

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(stream));
    String json = task.toJson();

    DatastreamTaskImpl task2 = DatastreamTaskImpl.fromJson(json);

    Assert.assertEquals(task2.getTaskPrefix(), stream.getName());
    Assert.assertTrue(task2.getDatastreamTaskName().contains(stream.getName()));
    Assert.assertEquals(task2.getConnectorType(), stream.getConnectorName());
  }

  @Test
  public void testTaskStatusJsonIO() {
    String json = JsonUtils.toJson(DatastreamTaskStatus.error("test msg"));
    JsonUtils.fromJson(json, DatastreamTaskStatus.class);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testErrorFromZkJson() throws Exception {
    Datastream stream = DatastreamTestUtils.createDatastream("dummy", "dummy", "dummy");
    stream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(stream));

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(stream));
    String json = task.toJson();
    DatastreamTaskImpl task2 = DatastreamTaskImpl.fromJson(json);
    task2.getDatastreams();
  }

  @Test
  public void testDatastreamTaskComparison() {
    Datastream stream = DatastreamTestUtils.createDatastream("dummy", "dummy", "dummy");
    stream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(stream));

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(stream), "dummyId", new ArrayList<>());
    DatastreamTaskImpl task2 = new DatastreamTaskImpl(Collections.singletonList(stream), "dummyId", new ArrayList<>());
    Assert.assertEquals(task, task2);

    task.setPartitions(Arrays.asList(1, 2));
    task2.setPartitions(Arrays.asList(2, 1, 3));
    Assert.assertNotEquals(task, task2);
    task2.setPartitions(Arrays.asList(2, 1));
    Assert.assertEquals(task, task2);

    task.setPartitionsV2(Arrays.asList("1", "2"));
    task2.setPartitionsV2(Arrays.asList("2", "1", "3"));
    Assert.assertNotEquals(task, task2);
    task2.setPartitionsV2(Arrays.asList("2", "1"));
    Assert.assertEquals(task, task2);
  }
}
