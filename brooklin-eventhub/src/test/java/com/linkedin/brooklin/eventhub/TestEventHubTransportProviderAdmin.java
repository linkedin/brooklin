package com.linkedin.brooklin.eventhub;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.api.transport.TransportProvider;


public class TestEventHubTransportProviderAdmin {

  public static final String DESTINATION = "eh://spunurutest2/eventhubut1_p8";
  private static final int DESTINATION_PARTITIONS = 4;
  public static final String EVENTHUB_KEY_NAME = "RootManageSharedAccessKey";
  public static final String EVENTHUB_KEY = "VJhm+uSYQqDZ1WUN5yeraFiDJg+6BGuNxoqy/L/JVDw=";

  public static Datastream createEventHubDatastream(String datastreamName) {
    Datastream ds = new Datastream();
    ds.setName(datastreamName);
    ds.setConnectorName(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString("source");
    ds.setDestination(new DatastreamDestination());
    ds.getDestination().setConnectionString(DESTINATION);
    ds.getDestination().setPartitions(DESTINATION_PARTITIONS);
    ds.setStatus(DatastreamStatus.READY);
    StringMap metadata = new StringMap();
    metadata.put(EventHubDestination.SHARED_ACCESS_KEY_NAME, EVENTHUB_KEY_NAME);
    metadata.put(EventHubDestination.SHARED_ACCESS_KEY, EVENTHUB_KEY);
    ds.setMetadata(metadata);
    return ds;
  }

  @Test
  public void testAssignTask() {
    EventHubTransportProviderAdmin tpAdmin = new EventHubTransportProviderAdmin("eh", new Properties());
    Datastream ds1 = createEventHubDatastream("ds1");
    List<Integer> partitions = IntStream.range(0, 4).boxed().collect(Collectors.toList());
    DatastreamTask task1 = new DatastreamTaskImpl(ds1, "ds1_1", partitions);
    TransportProvider transportProvider1 = tpAdmin.assignTransportProvider(task1);
    Assert.assertNotNull(transportProvider1);
    Assert.assertTrue(transportProvider1 instanceof EventHubTransportProvider);
    TransportProvider transportProvider2 = tpAdmin.assignTransportProvider(task1);
    Assert.assertEquals(transportProvider1, transportProvider2);
    DatastreamTask task2 = new DatastreamTaskImpl(ds1, "ds1_2", partitions);
    TransportProvider transportProvider3 = tpAdmin.assignTransportProvider(task2);
    Assert.assertNotEquals(transportProvider1, transportProvider3);
  }

  @Test
  public void testInitializeDestinationForDatastream() throws DatastreamValidationException {
    Datastream ds1 = createEventHubDatastream("ds1");
    EventHubTransportProviderAdmin tpAdmin = new EventHubTransportProviderAdmin("eh", new Properties());
    tpAdmin.initializeDestinationForDatastream(ds1);
  }

  @Test
  public void testInitializeDestinationForDatastreamWithInvalidDestination() {
    Datastream ds1 = createEventHubDatastream("ds1");
    EventHubTransportProviderAdmin tpAdmin = new EventHubTransportProviderAdmin("eh", new Properties());
    String connectionString = ds1.getDestination().getConnectionString();
    ds1.getDestination().setConnectionString("");
    try {
      tpAdmin.initializeDestinationForDatastream(ds1);
      Assert.fail();
    } catch (DatastreamValidationException e) {
    }

    ds1.getDestination().setConnectionString(connectionString);
    ds1.getMetadata().remove(EventHubDestination.SHARED_ACCESS_KEY_NAME);
    ds1.getMetadata().remove(EventHubDestination.SHARED_ACCESS_KEY);

    try {
      tpAdmin.initializeDestinationForDatastream(ds1);
      Assert.fail();
    } catch (DatastreamValidationException e) {
    }
  }

  @Test
  public void testUnassignTask() {
    EventHubTransportProviderAdmin tpAdmin = new EventHubTransportProviderAdmin("eh", new Properties());
    Datastream ds1 = createEventHubDatastream("ds1");
    List<Integer> partitions = IntStream.range(0, 4).boxed().collect(Collectors.toList());
    DatastreamTask task1 = new DatastreamTaskImpl(ds1, "ds1_1", partitions);
    tpAdmin.assignTransportProvider(task1);
    tpAdmin.unassignTransportProvider(task1);
  }
}
