package com.linkedin.datastream.server;

import java.util.Collections;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import org.testng.Assert;
import org.testng.annotations.Test;


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
}
