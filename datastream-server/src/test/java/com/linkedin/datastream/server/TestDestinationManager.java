package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.api.transport.TransportProvider;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


public class TestDestinationManager {

  public static Datastream generateDatastream(int seed) {
    return generateDatastream(seed, false);
  }

  public static Datastream generateDatastream(int seed, boolean withDestination) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorType(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString("DummySource_" + seed);
    ds.setDestination(new DatastreamDestination());
    if (withDestination) {
      ds.getDestination().setConnectionString("Destination_" + seed);
    }
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  public static TransportProvider createTransport() throws Exception {
    TransportProvider transport = mock(TransportProvider.class);
    doAnswer(invocation -> "transport://" + invocation.getArguments()[0]).when(transport).createTopic(
            anyString(), anyInt(), anyObject());
    return transport;
  }

  @Test
  public void testDestinationURI() throws Exception {
    Datastream datastream = generateDatastream(0);
    datastream.getSource().setConnectionString("connector:/no-authority/cluster/db/table/partition");
    TransportProvider transport = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transport);
    targetManager.populateDatastreamDestination(Collections.singletonList(datastream));
    String destination = datastream.getDestination().getConnectionString();
    Assert.assertTrue(destination.contains(DummyConnector.CONNECTOR_TYPE));
    Assert.assertTrue(destination.contains("noauthority"));
    Assert.assertTrue(destination.contains("cluster"));
    Assert.assertTrue(destination.contains("db"));
    Assert.assertTrue(destination.contains("table"));
    Assert.assertTrue(destination.contains("partition"));
    UUID.fromString(destination.substring(destination.lastIndexOf("_") + 1));
  }

  @Test
  public void testSourceWithNonAlphaChars() throws Exception {
    Datastream datastream = generateDatastream(0);
    datastream.getSource().setConnectionString("connector://authority/cluster/@/table/*");
    TransportProvider transport = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transport);
    targetManager.populateDatastreamDestination(Collections.singletonList(datastream));
    String destination = datastream.getDestination().getConnectionString();
    Assert.assertTrue(destination.contains(DummyConnector.CONNECTOR_TYPE));
    Assert.assertTrue(destination.contains("authority"));
    Assert.assertTrue(destination.contains("cluster"));
    Assert.assertTrue(destination.contains("table"));
    Assert.assertTrue(!destination.contains("*"));
    Assert.assertTrue(!destination.contains("@"));
    UUID.fromString(destination.substring(destination.lastIndexOf("_") + 1));
  }

  @Test
  public void testPopulateDatastreamDestinationUsesExistingTargetWhenSourceIsSame() throws Exception {
    List<Datastream> datastreams = new ArrayList<>();
    for (int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    Datastream newDatastream = generateDatastream(11);
    datastreams.add(newDatastream);
    newDatastream.setSource(datastreams.get(0).getSource());

    TransportProvider transport = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transport);
    targetManager.populateDatastreamDestination(datastreams);
    Assert.assertEquals(newDatastream.getDestination(), datastreams.get(0).getDestination());
  }

  @Test
  public void testPopulateDatastreamDestinationCallsCreateTopicWhenSourceIsSameButTopicReuseIsSetFalse()
      throws Exception {
    List<Datastream> datastreams = new ArrayList<>();
    for (int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    // Make 10 and 11 share the same source
    Datastream newDatastream = generateDatastream(11);
    // Disable destination reuse
    newDatastream.getMetadata().put(CoordinatorConfig.CONFIG_REUSE_EXISTING_DESTINATION, "false");
    datastreams.add(newDatastream);
    newDatastream.setSource(datastreams.get(0).getSource());

    TransportProvider transport = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transport);
    targetManager.populateDatastreamDestination(datastreams);

    // We should expect 11 topic creations for the 11 datastreams
    verify(transport, times(11)).createTopic(anyString(), anyInt(), any());
    Assert.assertNotEquals(newDatastream.getDestination(), datastreams.get(0).getDestination());
  }

  @Test
  public void testPopulateDatastreamDestinationCallsCreateTopicOnlyWhenDatastreamWithNewSource()
      throws Exception {
    List<Datastream> datastreams = new ArrayList<>();
    for (int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    // Make 1 and 2 share the same source ==> 9 distinct sources
    datastreams.get(2).setSource(datastreams.get(1).getSource());

    TransportProvider transport = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transport);
    targetManager.populateDatastreamDestination(datastreams);

    verify(transport, times(9)).createTopic(anyString(), anyInt(), any());
  }

  @Test
  public void testDeleteDatastreamDestinationShouldCallDropTopicWhenThereAreNoReferences() throws Exception {
    List<Datastream> datastreams = new ArrayList<>();
    for (int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    TransportProvider transportProvider = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transportProvider);
    targetManager.populateDatastreamDestination(datastreams);
    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(1)).dropTopic(eq(datastreams.get(1).getDestination().getConnectionString()));
  }

  @Test
  public void testDeleteDatastreamDestinationShouldNotCallDropTopicWhenThereAreReferences() throws Exception {
    List<Datastream> datastreams = new ArrayList<>();
    for (int index = 0; index < 10; index++) {
      datastreams.add(generateDatastream(index));
    }

    TransportProvider transportProvider = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transportProvider);
    targetManager.populateDatastreamDestination(datastreams);
    datastreams.get(0).setDestination(datastreams.get(1).getDestination());

    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(0)).dropTopic(eq(datastreams.get(1).getDestination().getConnectionString()));
  }
}
