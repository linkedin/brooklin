/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.dms;

import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.DatastreamRestClientFactory;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamStatus;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.RetriesExhaustedException;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamServer;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DummyTransportProviderAdminFactory;
import com.linkedin.datastream.server.EmbeddedDatastreamCluster;
import com.linkedin.datastream.server.TestDatastreamServer;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.ActionResult;
import com.linkedin.restli.server.BatchUpdateRequest;
import com.linkedin.restli.server.BatchUpdateResult;
import com.linkedin.restli.server.CreateResponse;
import com.linkedin.restli.server.PagingContext;
import com.linkedin.restli.server.PathKeys;
import com.linkedin.restli.server.RestLiServiceException;

import static com.linkedin.datastream.server.TestDatastreamServer.DUMMY_CONNECTOR;
import static com.linkedin.datastream.server.dms.DatastreamResources.CONFIG_STOP_TRANSITION_RETRY_PERIOD_MS;
import static com.linkedin.datastream.server.dms.DatastreamResources.CONFIG_STOP_TRANSITION_TIMEOUT_MS;
/**
 * Test DatastreamResources with ZooKeeper backed DatastreamStore
 */
@Test(singleThreaded = true)
public class TestDatastreamResources {

  private static final PagingContext NO_PAGING = new PagingContext(0, 0, false, false);
  private static final Duration TRANSITION_TIMEOUT_MS_DEFAULT = Duration.ofMillis(100);
  private static final Duration TRANSITION_RETRY_PERIOD_MS_DEFAULT = Duration.ofMillis(10);

  private EmbeddedDatastreamCluster _datastreamKafkaCluster;

  /**
   * Generate a datastream
   */
  public static Datastream generateDatastream(int seed) {
    return generateDatastream(seed, new HashSet<>());
  }

  /**
   * Generate a datastream
   * @param seed value to use in datastream properties
   * @param missingFields datastream properties to not set
   */
  public static Datastream generateDatastream(int seed, Set<String> missingFields) {
    Datastream ds = new Datastream();
    if (!missingFields.contains("name")) {
      ds.setName("name_" + seed);
    }
    if (!missingFields.contains("connectorType")) {
      ds.setConnectorName(DummyConnector.CONNECTOR_TYPE);
    }
    if (!missingFields.contains("source")) {
      ds.setSource(new DatastreamSource());
      ds.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    }

    if (!missingFields.contains("metadata")) {
      StringMap metadata = new StringMap();
      metadata.put(DatastreamMetadataConstants.OWNER_KEY, "person_" + seed);
      ds.setMetadata(metadata);
    }

    ds.setDestination(new DatastreamDestination());
    ds.setTransportProviderName(DummyTransportProviderAdminFactory.PROVIDER_NAME);
    return ds;
  }

  /**
   * Generate a datastream
   * @param seed value to use in datastream properties
   * @param setEncryptedMetadata Indicates whether {@value DatastreamMetadataConstants#DESTINATION_ENCRYPTION_REQUIRED}
   *                             should be set to {@code true} in the metadata of the returned datastream
   * @param setByotMetadata Indicates whether {@value DatastreamMetadataConstants#IS_USER_MANAGED_DESTINATION_KEY}
   *                        should be set to {@code true} in the metadata of the returned datastream
   */
  public static Datastream generateEncryptedDatastream(int seed, boolean setEncryptedMetadata,
      boolean setByotMetadata) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorName(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);

    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.OWNER_KEY, "person_" + seed);
    if (setEncryptedMetadata) {
      metadata.put(DatastreamMetadataConstants.DESTINATION_ENCRYPTION_REQUIRED, "true");
    }
    if (setByotMetadata) {
      metadata.put(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY, "true");
    }
    ds.setMetadata(metadata);

    ds.setDestination(new DatastreamDestination());
    ds.setTransportProviderName(DummyTransportProviderAdminFactory.PROVIDER_NAME);
    return ds;
  }

  @BeforeMethod
  public void setUp() throws Exception {
    _datastreamKafkaCluster = TestDatastreamServer.initializeTestDatastreamServerWithDummyConnector(null);
    _datastreamKafkaCluster.startup();
  }

  @AfterMethod
  public void cleanup() {
    _datastreamKafkaCluster.shutdown();
  }

  @Test
  public void testReadDatastream() throws Exception {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    // read before creating
    checkBadRequest(() -> resource1.get("name_0"), HttpStatus.S_404_NOT_FOUND);

    Datastream datastreamToCreate = generateDatastream(0);
    datastreamToCreate.setDestination(new DatastreamDestination());
    datastreamToCreate.getDestination().setConnectionString("testDestination");
    datastreamToCreate.getDestination().setPartitions(1);

    CreateResponse response = resource1.create(datastreamToCreate);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    Datastream ds = resource2.get("name_0");
    Assert.assertEquals(ds, datastreamToCreate);
  }

  @Test
  public void testGettingInstanceHostingTask() {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    datastreamToCreate.getDestination()
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);

    Assert.assertNull(resource1.create(datastreamToCreate).getError());

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    Coordinator testCoordinator = _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator();
    // waiting for leader to receive new datastream event
    PollUtils.poll(() -> testCoordinator.getDatastreamTasks().size() == 1, 100, 10000);

    String retrievedHost = null;
    String actualHost = testCoordinator.getInstanceName();

    Iterator<DatastreamTask> tasksIterator = testCoordinator.getDatastreamTasks().iterator();
    if (tasksIterator.hasNext()) {
      retrievedHost = resource2.getTaskAssignment(pathKey, tasksIterator.next().getDatastreamTaskName());
    }
    Assert.assertEquals(retrievedHost, actualHost);
  }

  @Test
  public void testPauseDatastream() {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DummyConnector connector = (DummyConnector) _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator()
        .getConnector(DUMMY_CONNECTOR);

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    datastreamToCreate.getDestination()
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    // Pause datastream.
    Assert.assertEquals(resource1.get(datastreamName).getStatus(), DatastreamStatus.READY);
    ActionResult<Void> pauseResponse = resource1.pause(pathKey, false);
    Assert.assertEquals(pauseResponse.getStatus(), HttpStatus.S_200_OK);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 2);

    // Retrieve datastream and check that is in pause state.
    Datastream ds = resource2.get(datastreamName);
    Assert.assertNotNull(ds);
    Assert.assertEquals(ds.getStatus(), DatastreamStatus.PAUSED);

    // Resume datastream.
    ActionResult<Void> resumeResponse = resource1.resume(pathKey, false);
    Assert.assertEquals(resumeResponse.getStatus(), HttpStatus.S_200_OK);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 3);

    // Retrieve datastream and check that is not paused.
    Datastream ds2 = resource2.get(datastreamName);
    Assert.assertNotNull(ds2);
    Assert.assertEquals(ds2.getStatus(), DatastreamStatus.READY);
  }

  @Test
  public void testStopDatastream() {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DummyConnector connector = (DummyConnector) _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator()
        .getConnector(DUMMY_CONNECTOR);

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    Objects.requireNonNull(datastreamToCreate.getDestination())
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    // Stop datastream.
    Assert.assertEquals(resource1.get(datastreamName).getStatus(), DatastreamStatus.READY);
    ActionResult<Void> stopResponse = resource1.stop(pathKey, false);
    Assert.assertEquals(stopResponse.getStatus(), HttpStatus.S_200_OK);

    // Retrieve datastream and check that is in STOPPED state.
    Datastream ds = resource2.get(datastreamName);
    Assert.assertNotNull(ds);
    Assert.assertEquals(ds.getStatus(), DatastreamStatus.STOPPED);
    // postDatastreamStateChangeAction should be invoked for status STOPPING and STOPPED
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 3);
  }

  @Test
  public void testSimultaneousStopDatastreamRequests() {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    Objects.requireNonNull(datastreamToCreate.getDestination())
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    Assert.assertEquals(resource1.get(datastreamName).getStatus(), DatastreamStatus.READY);

    // Stop datastream.
    ExecutorService executor = Executors.newFixedThreadPool(2);
    executor.execute(() -> resource1.stop(pathKey, false));
    executor.execute(() -> resource2.stop(pathKey, false));

    // Retrieve datastream and check that is in STOPPED state.
    Assert.assertTrue(PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.STOPPED, 100, 1000));
    executor.shutdownNow();
  }

  @Test
  public void testStopDatastreamWhenRequestHandlerHostDies() {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    Objects.requireNonNull(datastreamToCreate.getDestination())
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    // The executor attempts to stop a datastream which gets killed after the datastream is transitioned to STOPPING state.
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.execute(() -> {
      ActionResult<Void> stopResponse = resource1.stop(pathKey, false);
      Assert.assertEquals(stopResponse.getStatus(), HttpStatus.S_200_OK);
    });

    PollUtils.poll(() -> resource2.get(datastreamName).getStatus().equals(DatastreamStatus.STOPPING),
        inStoppingState -> inStoppingState, TRANSITION_RETRY_PERIOD_MS_DEFAULT.toMillis(),
        TRANSITION_TIMEOUT_MS_DEFAULT.toMillis());

    // Mocking crash of the request handler host.
    executor.shutdownNow();

    // Retrieve datastream and check that is in STOPPING state.
    Datastream ds = resource2.get(datastreamName);
    Assert.assertNotNull(ds);
    DatastreamStatus retrievedStatus = ds.getStatus();

    // If the datastream is found in STOPPED state, assuming that the leader already transitioned the datastream from STOPPING to STOPPED.
    if (retrievedStatus.equals(DatastreamStatus.STOPPED)) {
      return;
    }
    Assert.assertEquals(retrievedStatus, DatastreamStatus.STOPPING);

    // If the datastream is found in STOPPING state, there could be two cases here,
    // 1. The leader is notified before the host crashed, in that case the leader would transition the datastream back to STOPPED state.
    //    Hence polling in try block until the leader transitions the state of the datastream to STOPPED state.
    // 2. The host crashed before it could notify the leader, in that case we have to trigger stop call again, which is handled in catch block.
    try {
      PollUtils.poll(() -> resource2.get(datastreamName).getStatus().equals(DatastreamStatus.STOPPED),
          inStoppingState -> inStoppingState, TRANSITION_RETRY_PERIOD_MS_DEFAULT.toMillis(),
          TRANSITION_TIMEOUT_MS_DEFAULT.toMillis()).orElseThrow(RetriesExhaustedException::new);
    } catch (RetriesExhaustedException exception) {
      // Stop datastream.
      ActionResult<Void> stopResponse = resource1.stop(pathKey, false);
      Assert.assertEquals(stopResponse.getStatus(), HttpStatus.S_200_OK);
    } finally {
      // Retrieve datastream and check that is in STOPPED state.
      ds = resource2.get(datastreamName);
      Assert.assertNotNull(ds);
      Assert.assertEquals(ds.getStatus(), DatastreamStatus.STOPPED);
    }
  }

  @Test
  public void testPerformingAllActionsOnStoppingDatastream() throws DatastreamException {
    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamStore store = _datastreamKafkaCluster.getPrimaryDatastreamServer().getDatastreamStore();
    DummyConnector connector = (DummyConnector) _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator()
        .getConnector(DUMMY_CONNECTOR);

    // Create a Datastream
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    Objects.requireNonNull(datastreamToCreate.getDestination())
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    // Setting status to STOPPING explicitly to perform testing.
    datastreamToCreate.setStatus(DatastreamStatus.STOPPING);
    store.updateDatastream(datastreamName, datastreamToCreate, false);
    // as we are updating datastream directly on store, postDatastreamStateChangeActionn should not be invoked
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Retrieve datastream and check that is in STOPPING state.
    Datastream ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    Assert.assertEquals(ds.getStatus(), DatastreamStatus.STOPPING);
    // datastream get should not invoke postDatastreamStateChangeAction
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Attempting to pause a datastream in stopping state, which should raise an exception.
    Assert.assertEquals(
        Assert.expectThrows(RestLiServiceException.class, () -> resource1.pause(pathKey, false)).getStatus(),
        HttpStatus.S_405_METHOD_NOT_ALLOWED);
    // postDatastreamStateChangeAction should not be invoked
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Attempting to resume a datastream in stopping state, which should raise an exception.
    Assert.assertEquals(
        Assert.expectThrows(RestLiServiceException.class, () -> resource1.resume(pathKey, false)).getStatus(),
        HttpStatus.S_405_METHOD_NOT_ALLOWED);
    // postDatastreamStateChangeAction should not be invoked
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Attempting to stop a datastream in stopping state, which should get executed without exception.
    Assert.assertEquals(resource1.stop(pathKey, false).getStatus(), HttpStatus.S_200_OK);
    Assert.assertEquals(resource1.get(datastreamName).getStatus(), DatastreamStatus.STOPPED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 3);

    // Setting status to STOPPING again explicitly to perform testing.
    datastreamToCreate.setStatus(DatastreamStatus.STOPPING);
    store.updateDatastream(datastreamName, datastreamToCreate, false);

    // Retrieve datastream and check that is in STOPPING state.
    ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    Assert.assertEquals(ds.getStatus(), DatastreamStatus.STOPPING);

    // Attempting to delete a datastream in stopping state, which should get executed without exception.
    Assert.assertEquals(resource1.delete(datastreamName).getStatus(), HttpStatus.S_200_OK);
    Assert.assertEquals(resource1.get(datastreamName).getStatus(), DatastreamStatus.DELETING);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 4);
  }

  @Test
  public void testStopRequestTimeoutScenarioWithConfigurableTimeouts() {
    DatastreamServer testDatastreamServer = _datastreamKafkaCluster.getPrimaryDatastreamServer();
    DummyConnector connector = (DummyConnector) testDatastreamServer.getCoordinator().getConnector(DUMMY_CONNECTOR);

    // Configuring small timeouts to mock timeout scenario
    Properties testProperties = new Properties();
    testProperties.setProperty(CONFIG_STOP_TRANSITION_TIMEOUT_MS, "2");
    testProperties.setProperty(CONFIG_STOP_TRANSITION_RETRY_PERIOD_MS, "1");
    DatastreamResources resource1 = new DatastreamResources(testDatastreamServer.getDatastreamStore(), testDatastreamServer.getCoordinator(), testProperties);

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    Objects.requireNonNull(datastreamToCreate.getDestination())
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    // Stop datastream.
    Assert.assertEquals(resource1.get(datastreamName).getStatus(), DatastreamStatus.READY);

    // Attempting to stop a datastream which should timeout
    Assert.assertEquals(
        Assert.expectThrows(RestLiServiceException.class, () -> resource1.stop(pathKey, false)).getStatus(),
        HttpStatus.S_408_REQUEST_TIMEOUT);
    // postDatastreamStateChangeAction should be invoked only for STOPPING and not for STOPPED
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 2);
  }


  @Test
  public void testStopRequestTimeoutWithBusyLeader() throws DatastreamException {
    DatastreamServer testDatastreamServer = _datastreamKafkaCluster.getPrimaryDatastreamServer();
    DatastreamStore testDatastreamStore = testDatastreamServer.getDatastreamStore();
    DummyConnector connector = (DummyConnector) testDatastreamServer.getCoordinator().getConnector(DUMMY_CONNECTOR);

    // Attaching mock spies to the test instances of DatastreamCluster, DatastreamServer and DatastreamStore
    EmbeddedDatastreamCluster mockDatastreamCluster = Mockito.spy(_datastreamKafkaCluster);
    DatastreamServer mockDatastreamServer = Mockito.spy(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamStore mockDatastreamStore = Mockito.spy(testDatastreamStore);

    // Overriding methods to return mock spies
    Mockito.doReturn(mockDatastreamStore).when(mockDatastreamServer).getDatastreamStore();
    Mockito.doReturn(mockDatastreamServer).when(mockDatastreamCluster).getPrimaryDatastreamServer();

    DatastreamResources resource1 = new DatastreamResources(mockDatastreamCluster.getPrimaryDatastreamServer());

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();

    // Mocking the busy behavior of leader coordinator in update datastream call which won't notify the leader, and thus the request should timeout
    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      testDatastreamStore.updateDatastream((String) args[0], (Datastream) args[1], false); // Performing the same update action without notifying the leader
      return null;
    }).when(mockDatastreamStore).updateDatastream(Mockito.eq(datastreamName), Mockito.any(Datastream.class), Mockito.eq(true));

    datastreamToCreate.setDestination(new DatastreamDestination());
    Objects.requireNonNull(datastreamToCreate.getDestination())
        .setConnectionString("kafka://" + mockDatastreamCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    // Stop datastream.
    Assert.assertEquals(resource1.get(datastreamName).getStatus(), DatastreamStatus.READY);

    // Attempting to stop a datastream which should timeout
    Assert.assertEquals(
        Assert.expectThrows(RestLiServiceException.class, () -> resource1.stop(pathKey, false)).getStatus(),
        HttpStatus.S_408_REQUEST_TIMEOUT);
    // postDatastreamStateChangeAction should be invoked only for STOPPING and not for STOPPED
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 2);
  }

  @Test
  public void testPauseDatastreamGroup() throws Exception {
    // Create Two datastreams in the Same Group
    DatastreamRestClient restClient = createRestClient();
    Datastream ds1 = generateDatastream(1);
    restClient.createDatastream(ds1);
    restClient.waitTillDatastreamIsInitialized(ds1.getName(), 10000);

    Datastream ds2 = ds1.copy();
    ds2.setName("name_2");
    restClient.createDatastream(ds2);
    restClient.waitTillDatastreamIsInitialized(ds2.getName(), 10000);

    // Pause Datastream1 (normal)
    restClient.pause(ds1.getName(), false);
    ds1 = restClient.getDatastream(ds1.getName());
    ds2 = restClient.getDatastream(ds2.getName());
    Assert.assertEquals(ds1.getStatus(), DatastreamStatus.PAUSED);
    Assert.assertEquals(ds2.getStatus(), DatastreamStatus.READY);

    // Resume Datastream1 (Normal)
    restClient.resume(ds1.getName(), false);
    ds1 = restClient.getDatastream(ds1.getName());
    ds2 = restClient.getDatastream(ds2.getName());
    Assert.assertEquals(ds1.getStatus(), DatastreamStatus.READY);
    Assert.assertEquals(ds2.getStatus(), DatastreamStatus.READY);

    // Pause Datastream1 (Force)
    restClient.pause(ds1.getName(), true);
    ds1 = restClient.getDatastream(ds1.getName());
    ds2 = restClient.getDatastream(ds2.getName());
    Assert.assertEquals(ds1.getStatus(), DatastreamStatus.PAUSED);
    Assert.assertEquals(ds2.getStatus(), DatastreamStatus.PAUSED);

    // Resume Datastream1 (Force)
    restClient.resume(ds1.getName(), true);
    ds1 = restClient.getDatastream(ds1.getName());
    ds2 = restClient.getDatastream(ds2.getName());
    Assert.assertEquals(ds1.getStatus(), DatastreamStatus.READY);
    Assert.assertEquals(ds2.getStatus(), DatastreamStatus.READY);

    // STOPPED Datastream1 (normal)
    restClient.stop(ds1.getName());
    ds1 = restClient.getDatastream(ds1.getName());
    ds2 = restClient.getDatastream(ds2.getName());
    Assert.assertEquals(ds1.getStatus(), DatastreamStatus.STOPPED);
    Assert.assertEquals(ds2.getStatus(), DatastreamStatus.READY);

    // Resume Datastream1 (Normal)
    restClient.resume(ds1.getName(), false);
    ds1 = restClient.getDatastream(ds1.getName());
    ds2 = restClient.getDatastream(ds2.getName());
    Assert.assertEquals(ds1.getStatus(), DatastreamStatus.READY);
    Assert.assertEquals(ds2.getStatus(), DatastreamStatus.READY);

    // Do a call to find a datastream group
    List<Datastream> result = restClient.findGroup(ds2.getName());
    Assert.assertEquals(result.size(), 2);
    Assert.assertEquals(result.get(0).getName(), ds1.getName());
    Assert.assertEquals(result.get(1).getName(), ds2.getName());
  }

  @Test
  public void testPauseResumeSourcePartitions() throws Exception {
    final String topic1 = "topic1";
    final String topic2 = "topic2";
    final String topic3 = "topic3";
    final String nonExistentTopic = "nonExistentTopic";
    StringMap pausedPartitions = new StringMap();
    StringMap expectedPartitions = new StringMap();

    DatastreamResources resource1 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DatastreamResources resource2 = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    // Create a Datastream.
    Datastream datastreamToCreate = generateDatastream(0);
    String datastreamName = datastreamToCreate.getName();
    datastreamToCreate.setDestination(new DatastreamDestination());
    datastreamToCreate.getDestination()
        .setConnectionString("kafka://" + _datastreamKafkaCluster.getZkConnection() + "/testDestination");
    datastreamToCreate.getDestination().setPartitions(1);
    CreateResponse response = resource1.create(datastreamToCreate);
    PollUtils.poll(() -> resource1.get(datastreamName).getStatus() == DatastreamStatus.READY, 100, 10000);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Mock PathKeys
    PathKeys pathKey = Mockito.mock(PathKeys.class);
    Mockito.when(pathKey.getAsString(DatastreamResources.KEY_NAME)).thenReturn(datastreamName);

    // Make sure initial state is empty
    Assert.assertFalse(resource1.get(datastreamName)
        .getMetadata()
        .containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));

    // Now add "*" for topic1, "0" for topic2, "0" and "1" for topic3
    pausedPartitions.put(topic1, "*");
    pausedPartitions.put(topic2, "0");
    pausedPartitions.put(topic3, "1");
    expectedPartitions.putAll(pausedPartitions);
    ActionResult<Void> pausePartitionResponse = resource1.pauseSourcePartitions(pathKey, pausedPartitions);
    Assert.assertEquals(pausePartitionResponse.getStatus(), HttpStatus.S_200_OK);
    Datastream ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(expectedPartitions));

    // Add "0" and another "*" for topic1, and "1" for topic2.
    // Expect "0,*" ignored for topic1
    pausedPartitions.clear();
    pausedPartitions.put(topic1, "0,*");
    pausedPartitions.put(topic2, "1");
    pausePartitionResponse = resource1.pauseSourcePartitions(pathKey, pausedPartitions);
    Assert.assertEquals(pausePartitionResponse.getStatus(), HttpStatus.S_200_OK);
    ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    // Prepare expectedPartitions for validation.
    expectedPartitions.put(topic1, "0,*");
    expectedPartitions.put(topic2, "0,1");
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(expectedPartitions));

    // Now add "*" to topic2 - this should remove everything from topic2's list and add single
    // entry.
    pausedPartitions.clear();
    pausedPartitions.put(topic2, "*");
    pausePartitionResponse = resource1.pauseSourcePartitions(pathKey, pausedPartitions);
    Assert.assertEquals(pausePartitionResponse.getStatus(), HttpStatus.S_200_OK);
    ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    // Prepare expectedPartitions for validation.
    expectedPartitions.put(topic2, "0,1,*");
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(expectedPartitions));

    // Now resume "*" from topic2, "0" from topic3
    StringMap partitionsToResume = new StringMap();
    partitionsToResume.put(topic2, "*");
    partitionsToResume.put(topic3, "0");
    ActionResult<Void> resumePartitionResponse = resource1.resumeSourcePartitions(pathKey, partitionsToResume);
    Assert.assertEquals(resumePartitionResponse.getStatus(), HttpStatus.S_200_OK);
    ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    // Prepare expectedPartitions for validation.
    expectedPartitions.remove(topic2);
    expectedPartitions.put(topic3, "1");
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(expectedPartitions));

    // Now try resuming from a nonexistent topic.
    // This should be a no op, as there is nothing to resume.
    partitionsToResume.clear();
    partitionsToResume.put(nonExistentTopic, "*");
    resumePartitionResponse = resource1.resumeSourcePartitions(pathKey, partitionsToResume);
    Assert.assertEquals(resumePartitionResponse.getStatus(), HttpStatus.S_200_OK);
    ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(expectedPartitions));

    // Now remove "1" from topic3 - this should remove topic3 itself from the map
    partitionsToResume.clear();
    partitionsToResume.put(topic3, "1");
    resumePartitionResponse = resource1.resumeSourcePartitions(pathKey, partitionsToResume);
    Assert.assertEquals(resumePartitionResponse.getStatus(), HttpStatus.S_200_OK);
    ds = resource1.get(datastreamName);
    Assert.assertNotNull(ds);
    // Prepare pausedPartitions for validation
    expectedPartitions.remove(topic3);
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(expectedPartitions));
  }

  @Test
  public void testPauseResumeSourcePartitionsRestClient() throws Exception {
    final String topic1 = "topic1";
    final String topic2 = "topic2";
    final String topic3 = "topic3";

    // Create datastream
    DatastreamRestClient restClient = createRestClient();
    Datastream ds = generateDatastream(1);
    restClient.createDatastream(ds);
    restClient.waitTillDatastreamIsInitialized(ds.getName(), 10000);

    // Now add "*" for topic1, "0" for topic2, "0" and "1" for topic3
    StringMap pausedPartitions = new StringMap();
    pausedPartitions.put(topic1, "*");
    pausedPartitions.put(topic2, "0");
    pausedPartitions.put(topic3, "0,1");
    restClient.pauseSourcePartitions(ds.getName(), pausedPartitions);
    ds = restClient.getDatastream(ds.getName());
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(pausedPartitions));
    // Resume partitions
    StringMap resumePartitions = new StringMap();
    resumePartitions.put(topic1, "*");
    resumePartitions.put(topic2, "*");
    resumePartitions.put(topic3, "0");
    restClient.resumeSourcePartitions(ds.getName(), resumePartitions);
    // prepare pausedPartitions for validation
    pausedPartitions.remove(topic1);
    pausedPartitions.remove(topic2);
    pausedPartitions.put(topic3, "1");
    ds = restClient.getDatastream(ds.getName());
    Assert.assertTrue(ds.getMetadata().containsKey(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY));
    Assert.assertEquals(DatastreamUtils.getDatastreamSourcePartitions(ds),
        DatastreamUtils.parseSourcePartitionsStringMap(pausedPartitions));

    // Pause Datastream1 (normal)
    restClient.pause(ds.getName(), false);
    ds = restClient.getDatastream(ds.getName());
    Assert.assertEquals(ds.getStatus(), DatastreamStatus.PAUSED);

    // Now make sure that we receive an error on pausing partitions
    boolean receivedException = false;
    try {
      restClient.pauseSourcePartitions(ds.getName(), pausedPartitions);
    } catch (Exception e) {
      receivedException = true;
    }
    Assert.assertTrue(receivedException);

    // Now make sure that we receive an error on resuming partitions
    receivedException = false;
    try {
      restClient.resumeSourcePartitions(ds.getName(), pausedPartitions);
    } catch (Exception e) {
      receivedException = true;
    }
    Assert.assertTrue(receivedException);
  }

  private <T> void checkBadRequest(Callable<T> verif) throws Exception {
    checkBadRequest(verif, HttpStatus.S_400_BAD_REQUEST);
  }

  private <T> void checkBadRequest(Callable<T> verif, HttpStatus status) throws Exception {
    try {
      verif.call();
      Assert.fail();
    } catch (RestLiServiceException e) {
      Assert.assertEquals(e.getStatus(), status);
    }
  }

  private Datastream createAndWaitUntilInitialized(DatastreamResources resources, Datastream ds) {
    resources.create(ds);
    Assert.assertTrue(
        PollUtils.poll(() -> resources.get(ds.getName()).getStatus().equals(DatastreamStatus.READY), 100, 10000));
    return resources.get(ds.getName());
  }

  @Test
  public void testUpdateDatastream() throws Exception {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DummyConnector connector = (DummyConnector) _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator()
        .getConnector(DUMMY_CONNECTOR);

    Datastream originalDatastream1 = generateDatastream(1);

    checkBadRequest(() -> resource.update("none", originalDatastream1), HttpStatus.S_400_BAD_REQUEST);
    checkBadRequest(() -> resource.update(originalDatastream1.getName(), originalDatastream1),
        HttpStatus.S_404_NOT_FOUND);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 0);

    Datastream datastream1 = createAndWaitUntilInitialized(resource, originalDatastream1);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Cant update destination / status / connector / transport provider
    Datastream modifyDestination = generateDatastream(1);
    modifyDestination.getDestination().setConnectionString("updated");
    checkBadRequest(() -> resource.update(modifyDestination.getName(), modifyDestination),
        HttpStatus.S_400_BAD_REQUEST);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    Datastream modifyStatus = generateDatastream(1);
    modifyStatus.setStatus(DatastreamStatus.PAUSED);
    checkBadRequest(() -> resource.update(modifyStatus.getName(), modifyStatus), HttpStatus.S_400_BAD_REQUEST);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    Datastream modifyConnector = generateDatastream(1);
    modifyStatus.setConnectorName("Random");
    checkBadRequest(() -> resource.update(modifyConnector.getName(), modifyConnector), HttpStatus.S_400_BAD_REQUEST);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    Datastream modifyTransport = generateDatastream(1);
    modifyStatus.setTransportProviderName("Random");
    checkBadRequest(() -> resource.update(modifyTransport.getName(), modifyTransport), HttpStatus.S_400_BAD_REQUEST);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    Datastream modifyNumTasks = generateDatastream(1);
    modifyNumTasks.getMetadata().put("numTasks", "10");
    checkBadRequest(() -> resource.update(modifyNumTasks.getName(), modifyNumTasks), HttpStatus.S_400_BAD_REQUEST);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Create another datastream that gets deduped to datastream1.
    Datastream originalDatastream2 = generateDatastream(2);
    originalDatastream2.getDestination().setConnectionString("a different destination");
    final Datastream datastream2 = createAndWaitUntilInitialized(resource, originalDatastream2);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 2);

    // Update metadata for all streams in the group. It is up to the connector to validate if metadata updates are
    // allowed. Dummy connector allows it
    datastream1.getMetadata().put("key", "value1");
    datastream2.getMetadata().put("key", "value2");

    BatchUpdateRequest<String, Datastream> batchRequest = new BatchUpdateRequest<>(
        Stream.of(datastream1, datastream2).collect(Collectors.toMap(Datastream::getName, ds -> ds)));
    try {
      BatchUpdateResult<String, Datastream> response = resource.batchUpdate(batchRequest);
      Assert.assertTrue(
          response.getResults().values().stream().allMatch(res -> res.getStatus().equals(HttpStatus.S_200_OK)));
      Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 4);
    } catch (RestLiServiceException e) {
      Assert.fail("Valid batch update request failed");
    }

    Assert.assertTrue(PollUtils.poll(() -> {
      Datastream updatedDatastream1 = resource.get(datastream1.getName());
      Datastream updatedDatastream2 = resource.get(datastream2.getName());
      return updatedDatastream1.getMetadata().get("key").equals("value1")
          && updatedDatastream2.getMetadata().get("key").equals("value2");
    }, 100, 10000));

    datastream1.getMetadata().put("key", "value11");
    datastream2.getDestination().setConnectionString("updated");
    BatchUpdateRequest<String, Datastream> request = new BatchUpdateRequest<>(
        Stream.of(datastream1, datastream2).collect(Collectors.toMap(Datastream::getName, ds -> ds)));
    try {
      resource.batchUpdate(request);
      Assert.fail("Should have failed");
    } catch (RestLiServiceException e) {
      // do nothing
    }
    // post datastream state change should not get called on update failure
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 4);

    // make sure that on a failed batch update even the valid datastream update doesn't go through
    Thread.sleep(200);
    Datastream updatedDatastream = resource.get(datastream1.getName());
    // we might get false positive result because of the zk delay
    // if we get flaky result here that means something is wrong
    Assert.assertEquals(updatedDatastream.getMetadata().get("key"), "value1");
  }

  @Test
  public void testCreateEncryptedDatastream() {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DummyConnector connector = (DummyConnector) _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator()
        .getConnector(DUMMY_CONNECTOR);
    // Happy Path
    Datastream encryptedDS = generateEncryptedDatastream(1, true, true);
    CreateResponse response = resource.create(encryptedDS);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // Regression for Byot
    Datastream justByotDS = generateEncryptedDatastream(3, false, true);
    DatastreamDestination destination = new DatastreamDestination().setConnectionString("http://localhost:21324/foo");
    justByotDS.setDestination(destination);
    response = resource.create(justByotDS);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 2);
  }

  @Test
  public void testCreateDatastream() throws Exception {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    Set<String> missingFields = new HashSet<>();
    DummyConnector connector = (DummyConnector) _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator()
        .getConnector(DUMMY_CONNECTOR);

    // happy path
    Datastream fullDatastream = generateDatastream(0);
    CreateResponse response = resource.create(fullDatastream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 1);

    // datastream names with leading and/or trailing whitespace are trimmed
    Datastream whitespaceDatastream = generateDatastream(1);
    String originalName = whitespaceDatastream.getName();
    // make sure the generated datastream name has no leading or tailing whitespace to begin with
    Assert.assertEquals(originalName, originalName.trim());
    whitespaceDatastream.setName(String.format(" %s ", whitespaceDatastream.getName()));  // Add whitespace to name
    response = resource.create(whitespaceDatastream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(response.getId(), originalName);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 2);

    missingFields.add("target");
    Datastream allRequiredFields = generateDatastream(2, missingFields);
    response = resource.create(allRequiredFields);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 3);

    // missing necessary fields
    missingFields.clear();
    missingFields.add("name");
    Datastream noName = generateDatastream(3, missingFields);
    checkBadRequest(() -> resource.create(noName));

    missingFields.clear();
    missingFields.add("connectorType");
    Datastream noConnectorType = generateDatastream(4, missingFields);
    checkBadRequest(() -> resource.create(noConnectorType));

    missingFields.clear();
    missingFields.add("source");
    Datastream noSource = generateDatastream(5, missingFields);
    checkBadRequest(() -> resource.create(noSource));

    missingFields.clear();
    missingFields.add("metadata");
    Datastream noMetadata = generateDatastream(6, missingFields);
    checkBadRequest(() -> resource.create(noMetadata));

    Datastream noOwner = generateDatastream(6);
    noOwner.getMetadata().remove("owner");
    checkBadRequest(() -> resource.create(noOwner));

    Datastream badConnector = generateDatastream(6);
    badConnector.setConnectorName("BadConnector");
    checkBadRequest(() -> resource.create(badConnector));

    Datastream badSource = generateDatastream(6);
    badSource.getSource().setConnectionString("BadSource");
    checkBadRequest(() -> resource.create(badSource));

    // creating existing Datastream
    checkBadRequest(() -> resource.create(allRequiredFields), HttpStatus.S_409_CONFLICT);

    Datastream undefinedProvider = generateDatastream(7);
    String undefinedProviderName = "whatsoever";
    undefinedProvider.setTransportProviderName(undefinedProviderName);
    try {
      resource.create(undefinedProvider);
      Assert.fail("Should have failed for undefined provider name");
    } catch (RestLiServiceException e) {
      Assert.assertNotNull(e.getMessage());
      Assert.assertTrue(e.getMessage().contains(undefinedProviderName));
    }

    // Test datastream creation with numTasks
    Datastream badDatastream = generateDatastream(0);
    badDatastream.getMetadata().put("numTasks", "100");
    checkBadRequest(() -> resource.create(badDatastream));
    System.out.println("KHS11= " + connector.getPostDSStatechangeActionInvokeCount());
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 3);
  }

  private Datastream createDatastream(DatastreamResources resource, String name, int seed) {
    Datastream stream = generateDatastream(seed, new HashSet<>(Collections.singletonList("name")));
    stream.setName(name + seed);
    CreateResponse response = resource.create(stream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);
    return stream;
  }

  private List<Datastream> createDatastreams(DatastreamResources resource, String prefix, int count) throws Exception {
    return IntStream.range(0, count).mapToObj(n -> createDatastream(resource, prefix, n)).collect(Collectors.toList());
  }

  @Test
  public void testCreateGetAllDatastreams() throws Exception {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());
    DummyConnector connector = (DummyConnector) _datastreamKafkaCluster.getPrimaryDatastreamServer().getCoordinator()
        .getConnector(DUMMY_CONNECTOR);

    Assert.assertEquals(resource.getAll(NO_PAGING).size(), 0);

    String datastreamName = "TestDatastream-";
    List<Datastream> datastreams = createDatastreams(resource, datastreamName, 10);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 10);

    // Get All
    Optional<List<Datastream>> result =
        PollUtils.poll(() -> resource.getAll(NO_PAGING), streams -> streams.size() == datastreams.size(), 100, 1000);

    Assert.assertTrue(result.isPresent());

    List<Datastream> queryStreams = result.get();

    queryStreams.forEach(queryStream -> Assert.assertNotNull(queryStream.getDestination()));

    // Compare datastreams set only by name since destination is empty upon creation and later populated
    Assert.assertEquals(datastreams.stream().map(Datastream::getName).collect(Collectors.toSet()),
        queryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()));

    // Delete one entry
    Datastream removed = queryStreams.remove(0);
    Assert.assertEquals(resource.delete(removed.getName()).getStatus(), HttpStatus.S_200_OK);
    Assert.assertEquals(connector.getPostDSStatechangeActionInvokeCount(), 11);

    // Get All
    List<Datastream> remainingQueryStreams = resource.getAll(NO_PAGING)
        .stream()
        .filter(x -> x.getStatus() != DatastreamStatus.DELETING)
        .collect(Collectors.toList());
    // getAll should not invoke postDatastreamStateChangeAction

    // Compare datastreams set only by name since destination is empty upon creation and later populated
    Assert.assertEquals(queryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()),
        remainingQueryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()));
  }

  // This test is flaky, Need to deflake this before enabling the test.
  // This doesn't fail often, So need to run several times before you can catch the flakiness.
  @Test(enabled = false)
  public void testCreateGetAllDatastreamsPaging() throws Exception {
    DatastreamResources resource = new DatastreamResources(_datastreamKafkaCluster.getPrimaryDatastreamServer());

    Assert.assertEquals(resource.getAll(NO_PAGING).size(), 0);

    List<Datastream> datastreams = createDatastreams(resource, "TestDatastream-", 10);

    int skip = 2;
    int limit = 5;

    // Get All
    List<Datastream> queryStreams = resource.getAll(new PagingContext(skip, limit));
    queryStreams.forEach(queryStream -> Assert.assertNotNull(queryStream.getDestination()));

    // Compare datastreams set only by name since destination is empty upon creation and later populated
    Assert.assertEquals(
        datastreams.stream().map(Datastream::getName).skip(skip).limit(limit).collect(Collectors.toSet()),
        queryStreams.stream().map(Datastream::getName).collect(Collectors.toSet()));
  }

  private DatastreamRestClient createRestClient() {
    String dmsUri = String.format("http://localhost:%d", _datastreamKafkaCluster.getDatastreamPorts().get(0));
    return DatastreamRestClientFactory.getClient(dmsUri);
  }
}
