package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.api.transport.TransportException;
import com.linkedin.datastream.server.api.transport.TransportProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class TestDestinationManager {

  public static Datastream generateDatastream(int seed) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorType(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString("DummySource_" + seed);
    ds.setDestination(new DatastreamDestination());
    ds.getDestination().setConnectionString("Destination_" + seed);
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }
  @Test
  public void testPopulateDatastreamDestination_UsesExistingTarget_WhenSourceIsSame()
      throws TransportException {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    Datastream newDatastream = generateDatastream(11);
    datastreams.add(newDatastream);
    newDatastream.setSource(datastreams.get(0).getSource());
    newDatastream.removeDestination();
    DestinationManager targetManager = new DestinationManager(null);
    targetManager.populateDatastreamDestination(datastreams);
    Assert.assertEquals(newDatastream.getDestination(), datastreams.get(0).getDestination());
  }

  @Test
  public void testPopulateDatastreamDestination_CallsCreateTopic_OnlyWhenDatastreamWithNewSource()
      throws TransportException {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    datastreams.get(0).removeDestination();
    datastreams.get(1).removeDestination();
    datastreams.get(2).removeDestination();
    datastreams.get(2).setSource(datastreams.get(1).getSource());

    TransportProvider transportProvider = mock(TransportProvider.class);
    when(transportProvider.createTopic(anyString(), anyInt(), any())).thenReturn("destination");

    DestinationManager targetManager = new DestinationManager(transportProvider);
    targetManager.populateDatastreamDestination(datastreams);

    verify(transportProvider, times(2)).createTopic(anyString(), anyInt(), any());
  }

  @Test
  public void testDeleteDatastreamDestination_ShouldCallDropTopic_WhenThereAreNoReferences()
      throws TransportException {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    TransportProvider transportProvider = mock(TransportProvider.class);
    DestinationManager targetManager = new DestinationManager(transportProvider);
    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(1)).dropTopic(eq(datastreams.get(1).getDestination().getConnectionString()));
  }

  @Test
  public void testDeleteDatastreamDestination_ShouldNotCallDropTopic_WhenThereAreReferences()
      throws TransportException {
    List<Datastream> datastreams = new ArrayList<>();
    for(int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    datastreams.get(0).setDestination(datastreams.get(1).getDestination());
    TransportProvider transportProvider = mock(TransportProvider.class);
    DestinationManager targetManager = new DestinationManager(transportProvider);
    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(0)).dropTopic(eq(datastreams.get(1).getDestination().getConnectionString()));
  }


}
