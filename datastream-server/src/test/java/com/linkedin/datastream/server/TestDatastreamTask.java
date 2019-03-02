/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


public class TestDatastreamTask {

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
}
