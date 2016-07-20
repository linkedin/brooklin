package com.linkedin.datastream.server;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import org.testng.Assert;
import org.testng.annotations.Test;

import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.api.transport.TransportProvider;


public class TestDestinationManager {
  private static final Duration RETENTION = Duration.ofDays(3);

  public static Datastream generateDatastream(int seed) {
    return generateDatastream(seed, false);
  }

  public static Datastream generateDatastream(int seed, boolean withDestination) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorName(DummyConnector.CONNECTOR_TYPE);
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
    doAnswer(invocation -> "transport://" + invocation.getArguments()[0]).when(transport).getDestination(anyString());

    doReturn(RETENTION).when(transport).getRetention(anyString());
    return transport;
  }

  @Test
  public void testDestinationURI() throws Exception {
    Datastream datastream = generateDatastream(0);
    datastream.getSource().setConnectionString("connector:/no-authority/cluster/db/table/partition");
    TransportProvider transport = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transport);
    targetManager.populateDatastreamDestination(datastream, Collections.emptyList());
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
    targetManager.populateDatastreamDestination(datastream, Collections.emptyList());
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
    TransportProvider transport = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transport);

    List<Datastream> datastreams = new ArrayList<>();
    for (int index = 0; index < 10; index++) {
      Datastream newds = generateDatastream(index);
      targetManager.populateDatastreamDestination(newds, datastreams);
      datastreams.add(newds);
    }

    Datastream newDatastream = generateDatastream(11);
    newDatastream.setSource(datastreams.get(0).getSource());
    targetManager.populateDatastreamDestination(newDatastream, datastreams);

    datastreams.add(newDatastream);
    Assert.assertEquals(newDatastream.getDestination(), datastreams.get(0).getDestination());
  }

  @Test
  public void testDeleteDatastreamDestinationShouldCallDropTopicWhenThereAreNoReferences() throws Exception {
    TransportProvider transportProvider = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transportProvider);

    List<Datastream> datastreams = new ArrayList<>();
    for (int index = 0; index < 10; index++) {
      Datastream newds = generateDatastream(index);
      targetManager.populateDatastreamDestination(newds, datastreams);
      datastreams.add(newds);
    }

    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(1)).dropTopic(eq(datastreams.get(1).getDestination().getConnectionString()));
  }

  @Test
  public void testDeleteDatastreamDestinationShouldNotCallDropTopicWhenThereAreReferences() throws Exception {
    TransportProvider transportProvider = createTransport();
    DestinationManager targetManager = new DestinationManager(true, transportProvider);

    List<Datastream> datastreams = new ArrayList<>();

    Datastream newds = generateDatastream(0);
    targetManager.populateDatastreamDestination(newds, datastreams);
    datastreams.add(newds);
    newds = generateDatastream(1);

    newds.setSource(datastreams.get(0).getSource());
    targetManager.populateDatastreamDestination(newds, datastreams);
    datastreams.add(newds);

    targetManager.deleteDatastreamDestination(datastreams.get(1), datastreams);

    verify(transportProvider, times(0)).dropTopic(eq(datastreams.get(1).getDestination().getConnectionString()));
  }

  @Test
  public void testDestinationRetention() throws Exception {
    DestinationManager destinationManager = new DestinationManager(true, createTransport());

    // Allow DestinationManager set up creationTime
    Datastream stream = generateDatastream(1);
    destinationManager.populateDatastreamDestination(stream, Collections.emptyList());
    destinationManager.createTopic(stream);

    // Make sure both timestamps are set
    Assert.assertNotNull(stream.getMetadata().getOrDefault(DatastreamMetadataConstants.DESTINATION_CREATION_MS, null));
    Assert.assertNotNull(stream.getMetadata().getOrDefault(DatastreamMetadataConstants.DESTINATION_RETENION_MS, null));

    String retentionMs = stream.getMetadata().get(DatastreamMetadataConstants.DESTINATION_RETENION_MS);
    Assert.assertEquals(retentionMs, String.valueOf(RETENTION.toMillis()));
  }

  @Test
  public void testSameSourceDiffConnectorShouldNotReuse() throws Exception {
    DestinationManager destinationManager = new DestinationManager(true, createTransport());

    Datastream stream1 = generateDatastream(1);
    destinationManager.populateDatastreamDestination(stream1, Collections.emptyList());

    // create another datastream with the same source but different connector type
    Datastream stream2 = generateDatastream(1);
    stream2.setConnectorName("Foobar");

    List<Datastream> datastreams = new ArrayList<>();
    datastreams.add(stream1);
    destinationManager.populateDatastreamDestination(stream2, datastreams);

    Assert.assertNotEquals(stream1.getDestination().getConnectionString(),
        stream2.getDestination().getConnectionString());
  }
}
