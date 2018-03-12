package com.linkedin.datastream;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.log4j.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.TestRestliClientBase;
import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamAlreadyExistsException;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamNotFoundException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.RetriesExhaustedExeption;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.restli.client.Request;
import com.linkedin.restli.client.Response;
import com.linkedin.restli.client.ResponseFuture;
import com.linkedin.restli.client.RestClient;
import com.linkedin.restli.client.RestLiResponseException;
import com.linkedin.restli.common.IdResponse;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;



@Test(singleThreaded = true)
public class TestDatastreamRestClient extends TestRestliClientBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamRestClient.class);

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
  public void testDatastreamUpdate() throws Exception {
    Datastream datastream = generateDatastream(1200);
    DatastreamRestClient restClient = createRestClient();
    restClient.createDatastream(datastream);

    try {
      restClient.updateDatastream(generateDatastream(1201));
      Assert.fail("Update should fail for non exist datastream");
    } catch (DatastreamRuntimeException e) {
      // do nothing
    }

    Datastream initializedDatastream =
        restClient.waitTillDatastreamIsInitialized(datastream.getName(), WAIT_TIMEOUT_MS);
    initializedDatastream.getMetadata().put("key", "testDatastreamUpdate");
    restClient.updateDatastream(initializedDatastream);
    Assert.assertTrue(PollUtils.poll(() -> restClient.getDatastream(initializedDatastream.getName())
        .getMetadata()
        .get("key")
        .equals("testDatastreamUpdate"), 100, 10000));

    Datastream datastream2 = generateDatastream(1201);
    restClient.createDatastream(datastream2);

    Datastream initializedDatastream2 =
        restClient.waitTillDatastreamIsInitialized(datastream2.getName(), WAIT_TIMEOUT_MS);
    initializedDatastream2.getMetadata().put("key", "testDatastreamUpdate2");
    initializedDatastream.getMetadata().put("key", "testDatastreamUpdate3");
    restClient.updateDatastream(Arrays.asList(initializedDatastream, initializedDatastream2));

    Assert.assertTrue(PollUtils.poll(() -> restClient.getDatastream(initializedDatastream.getName())
        .getMetadata()
        .get("key")
        .equals("testDatastreamUpdate3"), 100, 10000));

    Assert.assertTrue(PollUtils.poll(() -> restClient.getDatastream(initializedDatastream2.getName())
        .getMetadata()
        .get("key")
        .equals("testDatastreamUpdate2"), 100, 10000));
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

  /*
   * For timeout retry unit test, pick getDatastream and createDatastream which represent both
   * methods that have return value and methods that have no return value (void)
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testCreateDatastreamRetrySucceed() throws Exception {
    Datastream datastream = generateDatastream(20);
    RestClient httpRestClient = mock(RestClient.class);
    ResponseFuture<IdResponse<String>> timeoutResponse = mock(ResponseFuture.class);
    ResponseFuture<IdResponse<String>> goodResponse = mock(ResponseFuture.class);
    when(httpRestClient.sendRequest(any(Request.class))).thenReturn(timeoutResponse, timeoutResponse, goodResponse);
    when(timeoutResponse.getResponse()).thenThrow(new RemoteInvocationException(new TimeoutException()));
    when(goodResponse.getResponse()).thenReturn(mock(Response.class));

    Properties restClientConfig = new Properties();
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_PERIOD_MS, "10");
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_TIMEOUT_MS, "10000");
    DatastreamRestClient restClient = new DatastreamRestClient(httpRestClient, restClientConfig);
    restClient.createDatastream(datastream);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetDatastreamRetrySucceed() throws Exception {
    Datastream datastream = generateDatastream(20);
    RestClient httpRestClient = mock(RestClient.class);
    ResponseFuture<Datastream> timeoutResponse = mock(ResponseFuture.class);
    ResponseFuture<Datastream> goodResponse = mock(ResponseFuture.class);
    when(httpRestClient.sendRequest(any(Request.class))).thenReturn(timeoutResponse, timeoutResponse, goodResponse);
    when(timeoutResponse.getResponseEntity()).thenThrow(new RemoteInvocationException(new TimeoutException()));
    when(goodResponse.getResponseEntity()).thenReturn(datastream);

    Properties restClientConfig = new Properties();
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_PERIOD_MS, "10");
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_TIMEOUT_MS, "10000");
    DatastreamRestClient restClient = new DatastreamRestClient(httpRestClient, restClientConfig);
    Assert.assertEquals(restClient.getDatastream(datastream.getName()).getSource().getConnectionString(),
        datastream.getSource().getConnectionString());
  }

  @Test(expectedExceptions = RetriesExhaustedExeption.class)
  @SuppressWarnings("unchecked")
  public void testCreateDatastreamRetryExhuast() throws Exception {
    Datastream datastream = generateDatastream(20);
    RestClient httpRestClient = mock(RestClient.class);
    ResponseFuture<IdResponse<String>> response = mock(ResponseFuture.class);
    when(httpRestClient.sendRequest(any(Request.class))).thenReturn(response);
    when(response.getResponse()).thenThrow(new RemoteInvocationException(new TimeoutException()));
    Properties restClientConfig = new Properties();
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_PERIOD_MS, "10");
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_TIMEOUT_MS, "200");
    DatastreamRestClient restClient = new DatastreamRestClient(httpRestClient, restClientConfig);
    restClient.createDatastream(datastream);
  }

  @Test(expectedExceptions = RetriesExhaustedExeption.class)
  @SuppressWarnings("unchecked")
  public void testGetDatastreamRetryExhuast() throws Exception {
    RestClient httpRestClient = mock(RestClient.class);
    ResponseFuture<IdResponse<String>> response = mock(ResponseFuture.class);
    when(httpRestClient.sendRequest(any(Request.class))).thenReturn(response);
    when(response.getResponse()).thenThrow(new RemoteInvocationException(new TimeoutException()));
    Properties restClientConfig = new Properties();
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_PERIOD_MS, "10");
    restClientConfig.put(DatastreamRestClient.CONFIG_RETRY_TIMEOUT_MS, "200");
    DatastreamRestClient restClient = new DatastreamRestClient(httpRestClient, restClientConfig);
    restClient.getDatastream("datastreamName");
  }
}
