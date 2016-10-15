package com.linkedin.datastream;

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
import com.linkedin.datastream.common.NetworkUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.connectors.DummyBootstrapConnectorFactory;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.DummyTransportProviderFactory;
import com.linkedin.datastream.server.assignment.BroadcastStrategyFactory;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.restli.client.RestLiResponseException;


@Test(singleThreaded = true)
public class TestDatastreamRestClient {
  private static final String TRANSPORT_FACTORY_CLASS = DummyTransportProviderFactory.class.getTypeName();

  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamRestClient.class);

  private static final String DUMMY_CONNECTOR = DummyConnector.CONNECTOR_TYPE;
  private static final String DUMMY_BOOTSTRAP_CONNECTOR = DummyBootstrapConnector.CONNECTOR_NAME;
  private static final long WAIT_TIMEOUT_MS = Duration.ofMinutes(3).toMillis();

  private DatastreamServer _datastreamServer;
  private EmbeddedZookeeper _embeddedZookeeper;

  @BeforeTest
  public void setUp() throws Exception {
    org.apache.log4j.Logger.getRootLogger().setLevel(Level.INFO);
    setupServer();
  }

  @AfterTest
  public void tearDown() throws Exception {
    _datastreamServer.shutdown();
    _embeddedZookeeper.shutdown();
  }

  public static Datastream generateDatastream(int seed) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorName(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString(String.format("%s://%s", DummyConnector.CONNECTOR_TYPE, "DummySource"));
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  private void setupServer() throws Exception {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _embeddedZookeeper.startup();
    setupDatastreamServer(NetworkUtils.getAvailablePort());
  }

  private DatastreamRestClient createRestClient() {
    return new DatastreamRestClient("http://localhost:" + _datastreamServer.getHttpPort());
  }

  private void setupDatastreamServer(int port) throws DatastreamException {
    String zkConnectionString = _embeddedZookeeper.getConnection();
    Properties properties = new Properties();
    properties.put(DatastreamServer.CONFIG_CLUSTER_NAME, "testCluster");
    properties.put(DatastreamServer.CONFIG_ZK_ADDRESS, zkConnectionString);
    properties.put(DatastreamServer.CONFIG_HTTP_PORT, String.valueOf(port));
    properties.put(DatastreamServer.CONFIG_CONNECTOR_NAMES, DUMMY_CONNECTOR + "," + DUMMY_BOOTSTRAP_CONNECTOR);
    properties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, TRANSPORT_FACTORY_CLASS);
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyConnectorFactory.class.getTypeName());
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_BOOTSTRAP_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyBootstrapConnectorFactory.class.getTypeName());
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_BOOTSTRAP_TYPE, DUMMY_BOOTSTRAP_CONNECTOR);
    // DummyConnector will verify this value being correctly set
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + ".dummyProperty", "dummyValue");
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY, BroadcastStrategyFactory.class.getTypeName());
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_BOOTSTRAP_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY, BroadcastStrategyFactory.class.getTypeName());
    _datastreamServer = new DatastreamServer(properties);
    _datastreamServer.startup();
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
    int httpPort = NetworkUtils.getAvailablePort();
    setupDatastreamServer(httpPort);
    Datastream datastream = generateDatastream(5);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:" + httpPort);
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
    restClient.deleteDatastream(datastream.getName());
    restClient.getDatastream(datastream.getName());
  }

  @Test
  public void testCreateBootstrapDatastream() throws Exception {
    Datastream bootstrapDatastream = generateDatastream(3);
    LOG.info("Bootstrap datastream : " + bootstrapDatastream);
    DatastreamRestClient restClient = createRestClient();
    restClient.createBootstrapDatastream(bootstrapDatastream);
    Datastream createdDatastream = restClient.getDatastream(bootstrapDatastream.getName());
    LOG.info("Created Datastream : " + createdDatastream);
    Assert.assertEquals(bootstrapDatastream.getName(), createdDatastream.getName());
    Assert.assertEquals(bootstrapDatastream.getConnectorName(), createdDatastream.getConnectorName());
  }

  @Test(expectedExceptions = DatastreamAlreadyExistsException.class)
  public void testCreateBootstrapDatastreamThatAlreadyExists() {
    Datastream bootstrapDatastream = generateDatastream(4);
    LOG.info("Bootstrap datastream : " + bootstrapDatastream);
    DatastreamRestClient restClient = createRestClient();
    restClient.createBootstrapDatastream(bootstrapDatastream);
    restClient.createBootstrapDatastream(bootstrapDatastream);
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
}
