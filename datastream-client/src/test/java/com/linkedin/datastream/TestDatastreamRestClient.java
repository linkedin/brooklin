package com.linkedin.datastream;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamNotFoundException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.assignment.BroadcastStrategyFactory;
import com.linkedin.restli.client.RestLiResponseException;


@Test(singleThreaded = true)
public class TestDatastreamRestClient {
  private static final String TRANSPORT_NAME = "default";

  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamRestClient.class);

  private static final long WAIT_TIMEOUT_MS = Duration.ofMinutes(3).toMillis();

  private EmbeddedDatastreamCluster _datastreamCluster;

  @BeforeTest
  public void setUp() throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);

    // Create a cluster with maximum 2 DMS instances
    setupDatastreamCluster(2);
  }

  @AfterTest
  public void tearDown() throws Exception {
    _datastreamCluster.shutdown();
  }

  private void setupDatastreamCluster(int numServers) throws IOException, DatastreamException {
    Properties connectorProps = new Properties();
    connectorProps.put(DatastreamServer.CONFIG_FACTORY_CLASS_NAME, DummyConnectorFactory.class.getCanonicalName());
    connectorProps.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY,
        BroadcastStrategyFactory.class.getTypeName());
    connectorProps.put("dummyProperty", "dummyValue");

    _datastreamCluster = EmbeddedDatastreamCluster.newTestDatastreamCluster(
        Collections.singletonMap(DummyConnector.CONNECTOR_TYPE, connectorProps), null, numServers);

    // NOTE: Only start the first instance by default
    // Test case needing the 2nd one should start the 2nd instance
    _datastreamCluster.startupServer(0);
  }

  public static Datastream generateDatastream(int seed) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorName(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString(String.format("%s://%s", DummyConnector.CONNECTOR_TYPE, "DummySource"));
    ds.setDestination(new DatastreamDestination());
    ds.setTransportProviderName(TRANSPORT_NAME);
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  /**
   * Create a rest client with the default/leader DMS instance
   * @return
   */
  private DatastreamRestClient createRestClient() {
    String dmsUri = String.format("http://localhost:%d", _datastreamCluster.getDatastreamPorts().get(0));
    return DatastreamRestClientFactory.getClient(dmsUri);
  }

  @Test
  public void testCreateTwoDatastreams() throws Exception {
    Datastream datastream = generateDatastream(6);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = createRestClient();
    restClient.createDatastream(datastream);
    Datastream createdDatastream = restClient.waitTillDatastreamIsInitialized(datastream.getName(), WAIT_TIMEOUT_MS);
    LOG.info("Created Datastream : " + createdDatastream);

    datastream.setDestination(new DatastreamDestination());
    // server might have already set the destination so we need to unset it for comparison
    clearDatastreamDestination(Collections.singletonList(createdDatastream));
    clearDynamicMetadata(Collections.singletonList(createdDatastream));
    Assert.assertEquals(createdDatastream, datastream);

    datastream = generateDatastream(7);
    LOG.info("Datastream : " + datastream);
    restClient.createDatastream(datastream);
    createdDatastream = restClient.waitTillDatastreamIsInitialized(datastream.getName(), WAIT_TIMEOUT_MS);
    LOG.info("Created Datastream : " + createdDatastream);

    datastream.setDestination(new DatastreamDestination());
    // server might have already set the destination so we need to unset it for comparison
    clearDatastreamDestination(Collections.singletonList(createdDatastream));
    clearDynamicMetadata(Collections.singletonList(createdDatastream));
    Assert.assertEquals(createdDatastream, datastream);
  }

  @Test
  public void testCreateDatastreamToNonLeader() throws Exception {
    // Start the second DMS instance to be the follower
    _datastreamCluster.startupServer(1);

    Datastream datastream = generateDatastream(5);
    LOG.info("Datastream : " + datastream);

    int followerDmsPort = _datastreamCluster.getDatastreamPorts().get(1);
    DatastreamRestClient restClient = DatastreamRestClientFactory.getClient("http://localhost:" + followerDmsPort);
    restClient.createDatastream(datastream);
    Datastream createdDatastream = restClient.waitTillDatastreamIsInitialized(datastream.getName(), WAIT_TIMEOUT_MS);
    LOG.info("Created Datastream : " + createdDatastream);
    datastream.setDestination(new DatastreamDestination());
    // server might have already set the destination so we need to unset it for comparison
    clearDatastreamDestination(Collections.singletonList(createdDatastream));
    clearDynamicMetadata(Collections.singletonList(createdDatastream));
    Assert.assertEquals(createdDatastream, datastream);
  }

  @Test(expectedExceptions = DatastreamAlreadyExistsException.class)
  public void testCreateDatastreamThatAlreadyExists() throws Exception {
    Datastream datastream = generateDatastream(1);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = createRestClient();
    restClient.createDatastream(datastream);
    restClient.createDatastream(datastream);
  }

  @Test
  public void testWaitTillDatastreamIsInitializedReturnsInitializedDatastream() throws Exception {
    Datastream datastream = generateDatastream(11);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = createRestClient();
    restClient.createDatastream(datastream);
    Datastream initializedDatastream = restClient.waitTillDatastreamIsInitialized(datastream.getName(), 60000);
    LOG.info("Initialized Datastream : " + initializedDatastream);
    Assert.assertNotEquals(initializedDatastream.getDestination().getConnectionString(), "");
    Assert.assertEquals(initializedDatastream.getDestination().getPartitions().intValue(), 1);
  }

  private void clearDatastreamDestination(Collection<Datastream> datastreams) {
    for (Datastream datastream : datastreams) {
      datastream.setDestination(new DatastreamDestination());
      datastream.removeStatus();
    }
  }

  /**
   * Metadata are added dynamically by the server so we need to
   * remove them for the equality check with source datastreams.
   */
  private void clearDynamicMetadata(Collection<Datastream> datastreams) {
    for (Datastream stream : datastreams) {
      stream.getMetadata().remove(DatastreamMetadataConstants.DESTINATION_CREATION_MS);
      stream.getMetadata().remove(DatastreamMetadataConstants.DESTINATION_RETENION_MS);
      stream.getMetadata().remove(DatastreamMetadataConstants.CREATION_MS);
      stream.getMetadata().remove(DatastreamMetadataConstants.TASK_PREFIX);
    }
  }

  @Test
  public void testGetAllDatastreams() throws Exception {
    List<Datastream> datastreams =
        IntStream.range(100, 110).mapToObj(TestDatastreamRestClient::generateDatastream).collect(Collectors.toList());
    LOG.info("Datastreams : " + datastreams);
    DatastreamRestClient restClient = createRestClient();

    int initialSize = restClient.getAllDatastreams().size();
    int createdCount = datastreams.size();

    for (Datastream datastream : datastreams) {
      restClient.createDatastream(datastream);
    }

    Optional<List<Datastream>> result =
        PollUtils.poll(restClient::getAllDatastreams, streams -> streams.size() - initialSize == createdCount, 100,
            1000);

    Assert.assertTrue(result.isPresent());

    List<Datastream> createdDatastreams = result.get();
    LOG.info("Created Datastreams : " + createdDatastreams);

    clearDatastreamDestination(datastreams);
    clearDatastreamDestination(createdDatastreams);
    clearDynamicMetadata(createdDatastreams);

    Assert.assertTrue(new HashSet<>(createdDatastreams).containsAll(datastreams), "Original datastreams " +
        datastreams + " not present in last getAll " + createdDatastreams);

    int skip = 2;
    int count = 5;
    List<Datastream> paginatedCreatedDatastreams = restClient.getAllDatastreams(2, 5);
    LOG.info("Paginated Datastreams : " + paginatedCreatedDatastreams);

    Assert.assertEquals(paginatedCreatedDatastreams.size(), count);

    clearDatastreamDestination(paginatedCreatedDatastreams);
    clearDynamicMetadata(paginatedCreatedDatastreams);

    Assert.assertEquals(createdDatastreams.stream().skip(skip).limit(count).collect(Collectors.toList()),
        paginatedCreatedDatastreams);
  }

  @Test(expectedExceptions = DatastreamNotFoundException.class)
  public void testDeleteDatastream() throws Exception {
    Datastream datastream = generateDatastream(2);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = createRestClient();
    restClient.createDatastream(datastream);
    restClient.waitTillDatastreamIsInitialized(datastream.getName(), Duration.ofMinutes(2).toMillis());
    restClient.deleteDatastream(datastream.getName());
    restClient.waitTillDatastreamIsDeleted(datastream.getName(), Duration.ofMinutes(2).toMillis());
    restClient.getDatastream(datastream.getName());
  }

  @Test(expectedExceptions = DatastreamNotFoundException.class)
  public void testGetDatastreamThrowsDatastreamNotFoundExceptionWhenDatastreamIsNotfound() throws Exception {
    DatastreamRestClient restClient = createRestClient();
    restClient.getDatastream("Datastream_doesntexist");
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testCreateDatastreamThrowsDatastreamExceptionOnBadDatastream() throws Exception {
    DatastreamRestClient restClient = createRestClient();
    restClient.createDatastream(new Datastream());
  }

  @Test
  public void testDatastreamExists() throws Exception {
    Datastream datastream = generateDatastream(1111);
    DatastreamRestClient restClient = createRestClient();
    restClient.createDatastream(datastream);
    Assert.assertNotNull(restClient.waitTillDatastreamIsInitialized(datastream.getName(), WAIT_TIMEOUT_MS));
    Assert.assertTrue(restClient.datastreamExists(datastream.getName()));
    Assert.assertFalse(restClient.datastreamExists("No Such Datastream"));
  }

  @Test
  public void testBadCreateThrowsWithServerErrorRetained() throws Exception {
    try {
      DatastreamRestClient restClient = createRestClient();
      restClient.createDatastream(new Datastream());
    } catch (DatastreamRuntimeException e) {
      RestLiResponseException re = (RestLiResponseException) e.getCause();
      Assert.assertNotNull(re.getServiceErrorMessage());
    }
  }

  @Test(expectedExceptions = DatastreamNotFoundException.class)
  public void testDeleteNonExistentDatastream() throws Exception {
    DatastreamRestClient restClient = createRestClient();
    restClient.deleteDatastream("NoSuchDatastream");
  }
}
