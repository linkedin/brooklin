package com.linkedin.brooklin.eventhub;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;


public class TestEventHubDestination {

  @Test
  public void testCreate() {
    String namespace = "ehns1";
    String hubname = "hub1";
    Datastream datastream = new Datastream();
    datastream.setName("test");
    datastream.setDestination(new DatastreamDestination());
    datastream.getDestination().setConnectionString(String.format("eh://%s/%s", namespace, hubname));
    datastream.setMetadata(new StringMap());
    datastream.getMetadata().put(EventHubDestination.SHARED_ACCESS_KEY_NAME, "eventHubKeyName");
    datastream.getMetadata().put(EventHubDestination.SHARED_ACCESS_KEY, "eventHubKey");

    EventHubDestination destination = new EventHubDestination(datastream);
    Assert.assertEquals(destination.getEventHubName(), hubname);
    Assert.assertEquals(destination.getEventHubNamespace(), namespace);
  }
}