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

    DatastreamTaskImpl task = new DatastreamTaskImpl(stream);
    String json = task.toJson();

    DatastreamTaskImpl task2 = DatastreamTaskImpl.fromJson(json);

    Assert.assertEquals(task2.getDatastreamName(), stream.getName());
    Assert.assertTrue(task2.getDatastreamTaskName().contains(stream.getName()));
    Assert.assertEquals(task2.getConnectorType(), stream.getConnectorType());
  }

}
