package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.connectors.DummyConnector;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestTopicManager {

  public static Datastream generateDatastream(int seed) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorType(DummyConnector.CONNECTOR_TYPE);
    ds.setSource("DummySource_" + seed);
    ds.setDestination("Destination_" + seed);
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }
  @Test
  public void testPopulateDatastreamDestination_UsesExistingTarget_WhenSourceIsSame() {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    Datastream newDatastream = generateDatastream(11);
    datastreams.add(newDatastream);
    newDatastream.setSource(datastreams.get(0).getSource());
    newDatastream.setDestination("");
    TopicManager targetManager = new TopicManager(null);
    targetManager.populateDatastreamDestination(datastreams);
    Assert.assertEquals(newDatastream.getDestination(), datastreams.get(0).getDestination());
  }

  @Test
  public void testPopulateDatastreamDestination_CallsCreateTopic_OnlyWhenDatastreamWithNewSource() {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    datastreams.get(0).setDestination("");
    datastreams.get(1).setDestination("");
    datastreams.get(2).setDestination("");
    datastreams.get(2).setSource(datastreams.get(1).getSource());

    TransportProvider transportProvider = mock(TransportProvider.class);
    when(transportProvider.createTopic(eq(datastreams.get(0).getName()), anyInt(), any())).thenReturn("destination1");
    when(transportProvider.createTopic(eq(datastreams.get(1).getName()), anyInt(), any())).thenReturn("destination2");

    TopicManager targetManager = new TopicManager(transportProvider);
    targetManager.populateDatastreamDestination(datastreams);

    verify(transportProvider, times(1)).createTopic(eq(datastreams.get(0).getName()), anyInt(), any());
    verify(transportProvider, times(1)).createTopic(eq(datastreams.get(1).getName()), anyInt(), any());
  }

  @Test
  public void testDeleteDatastreamDestination_ShouldCallDropTopic_WhenThereAreNoReferences() {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    TransportProvider transportProvider = mock(TransportProvider.class);
    TopicManager targetManager = new TopicManager(transportProvider);
    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(1)).dropTopic(eq(datastreams.get(1).getDestination()));
  }

  @Test
  public void testDeleteDatastreamDestination_ShouldNotCallDropTopic_WhenThereAreReferences() {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    datastreams.get(0).setDestination(datastreams.get(1).getDestination());
    TransportProvider transportProvider = mock(TransportProvider.class);
    TopicManager targetManager = new TopicManager(transportProvider);
    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(0)).dropTopic(eq(datastreams.get(1).getDestination()));
  }


}
