package com.linkedin.datastream;

import java.io.IOException;
import java.util.Collection;
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
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamNotFoundException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.connectors.DummyBootstrapConnectorFactory;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.server.CoordinatorConfig;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.DummyTransportProviderFactory;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.r2.RemoteInvocationException;


@Test(singleThreaded = true)
public class TestDatastreamRestClient {
  private static final String TRANSPORT_FACTORY_CLASS = DummyTransportProviderFactory.class.getTypeName();

  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamRestClient.class);

  private static final String DUMMY_CONNECTOR = DummyConnector.CONNECTOR_TYPE;
  private static final String DUMMY_BOOTSTRAP_CONNECTOR = DummyBootstrapConnector.CONNECTOR_TYPE;
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
    ds.setConnectorType(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString("DummySource");
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  private void setupServer() throws Exception {
    _embeddedZookeeper = new EmbeddedZookeeper();
    String zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();

    Properties properties = new Properties();
    properties.put(DatastreamServer.CONFIG_CLUSTER_NAME, "testCluster");
    properties.put(DatastreamServer.CONFIG_ZK_ADDRESS, zkConnectionString);
    properties.put(DatastreamServer.CONFIG_HTTP_PORT, "8080");
    properties.put(DatastreamServer.CONFIG_CONNECTOR_TYPES, DUMMY_CONNECTOR + "," + DUMMY_BOOTSTRAP_CONNECTOR);
    properties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, TRANSPORT_FACTORY_CLASS);
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyConnectorFactory.class.getTypeName());
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_BOOTSTRAP_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_FACTORY_CLASS_NAME, DummyBootstrapConnectorFactory.class.getTypeName());
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + "."
        + DatastreamServer.CONFIG_CONNECTOR_BOOTSTRAP_TYPE, DUMMY_BOOTSTRAP_CONNECTOR);
    properties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + ".dummyProperty",
        "dummyValue"); // DummyConnector will verify this value being correctly set
    properties.put(CoordinatorConfig.CONFIG_SCHEMA_REGISTRY_PROVIDER_FACTORY,
            "com.linkedin.datastream.server.MockSchemaRegistryProviderFactory");
    _datastreamServer = new DatastreamServer(properties);
    _datastreamServer.startup();
  }

  @Test
  public void testCreateDatastream() throws DatastreamException, IOException, RemoteInvocationException {
    Datastream datastream = generateDatastream(1);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.createDatastream(datastream);
    Datastream createdDatastream = restClient.getDatastream(datastream.getName());
    LOG.info("Created Datastream : " + createdDatastream);
    datastream.setDestination(new DatastreamDestination());
    // server might have already set the destination so we need to unset it for comparison
    createdDatastream.setDestination(new DatastreamDestination());
    Assert.assertEquals(createdDatastream, datastream);
  }

  @Test
  public void testWaitTillDatastreamIsInitializedReturnsInitializedDatastream()
      throws DatastreamException, InterruptedException {
    Datastream datastream = generateDatastream(11);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.createDatastream(datastream);
    Datastream initializedDatastream = restClient.waitTillDatastreamIsInitialized(datastream.getName(), 60000);
    LOG.info("Initialized Datastream : " + initializedDatastream);
    Assert.assertNotEquals(initializedDatastream.getDestination().getConnectionString(), "");
    Assert.assertEquals(initializedDatastream.getDestination().getPartitions().intValue(), 1);
  }

  private void clearDatastreamDestination(Collection<Datastream> datastreams) {
    for (Datastream datastream : datastreams) {
      datastream.setDestination(new DatastreamDestination());
    }
  }

  @Test
  public void testGetAllDatastreams() throws DatastreamException, IOException, RemoteInvocationException, InterruptedException {
    List<Datastream> datastreams =
        IntStream.range(100, 110).mapToObj(i -> generateDatastream(i)).collect(Collectors.toList());
    LOG.info("Datastreams : " + datastreams);
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");

    int initialSize = restClient.getAllDatastreams().size();
    int createdCount = datastreams.size();

    for (Datastream datastream : datastreams) {
      restClient.createDatastream(datastream);
    }

    Optional<List<Datastream>> result = PollUtils.poll(restClient::getAllDatastreams,
        streams -> streams.size() - initialSize == createdCount, 100, 1000);

    Assert.assertTrue(result.isPresent());

    List<Datastream> createdDatastreams = result.get();
    LOG.info("Created Datastreams : " + createdDatastreams);

    clearDatastreamDestination(datastreams);
    clearDatastreamDestination(createdDatastreams);

    Assert.assertTrue(new HashSet<>(createdDatastreams).containsAll(datastreams), "Original datastreams " +
        datastreams + " not present in last getAll " + createdDatastreams);

    int skip = 2;
    int count = 5;
    List<Datastream> paginatedCreatedDatastreams = restClient.getAllDatastreams(2, 5);
    LOG.info("Paginated Datastreams : " + paginatedCreatedDatastreams);

    Assert.assertEquals(paginatedCreatedDatastreams.size(), count);

    clearDatastreamDestination(paginatedCreatedDatastreams);

    Assert.assertEquals(createdDatastreams.stream().skip(skip).limit(count).collect(Collectors.toList()), paginatedCreatedDatastreams);
  }

  @Test(expectedExceptions = DatastreamNotFoundException.class)
  public void testDeleteDatastream() throws DatastreamException {
    Datastream datastream = generateDatastream(2);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.createDatastream(datastream);
    restClient.deleteDatastream(datastream.getName());
    restClient.getDatastream(datastream.getName());
  }

  @Test
  public void testGetBootstrapDatastream() throws IOException, DatastreamException, RemoteInvocationException {
    Datastream datastream = generateDatastream(3);
    LOG.info("Datastream : " + datastream);
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.createDatastream(datastream);
    Datastream createdDatastream = restClient.getDatastream(datastream.getName());
    LOG.info("Created Datastream : " + createdDatastream);
    Datastream bootstrapDatastream = restClient.createBootstrapDatastream(datastream.getName());
    Assert.assertEquals(bootstrapDatastream.getConnectorType(), DUMMY_BOOTSTRAP_CONNECTOR);
    Assert.assertTrue(bootstrapDatastream.getName().startsWith(datastream.getName()));
  }

  @Test(expectedExceptions = DatastreamNotFoundException.class)
  public void testGetBootstrapDatastreamThrowsDatastreamNotFoundExceptionWhenDatastreamIsNotfound() throws
      IOException, DatastreamException, RemoteInvocationException {
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.createBootstrapDatastream("Datastream_doesntexist");
  }

  @Test(expectedExceptions = DatastreamNotFoundException.class)
  public void testGetDatastreamThrowsDatastreamNotFoundExceptionWhenDatastreamIsNotfound() throws IOException,
      DatastreamException, RemoteInvocationException {
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.getDatastream("Datastream_doesntexist");
  }

  @Test(expectedExceptions = DatastreamRuntimeException.class)
  public void testCreateDatastreamThrowsDatastreamExceptionOnBadDatastream() throws IOException, DatastreamException,
      RemoteInvocationException {
    DatastreamRestClient restClient = new DatastreamRestClient("http://localhost:8080/");
    restClient.createDatastream(new Datastream());
  }
}
