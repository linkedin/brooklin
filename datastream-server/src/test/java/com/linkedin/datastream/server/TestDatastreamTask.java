package com.linkedin.datastream.server;

import com.linkedin.datastream.common.Datastream;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestDatastreamTask {

  @Test
  public void testDatastreamTaskJson() throws Exception {
    Datastream stream = new Datastream();
    stream.setName("testDatastreamTaskJsonName");
    stream.setConnectorType("testDatastreamTaskJson");

    DatastreamTask task = new DatastreamTask(stream);
    String json = task.toJson();

    DatastreamTask task2 = DatastreamTask.fromJson(json);

    Assert.assertEquals(task2.getDatastreamName(), stream.getName());
    Assert.assertEquals(task2.getDatastreamTaskName(), stream.getName());
    Assert.assertEquals(task2.getConnectorType(), stream.getConnectorType());
  }

}
