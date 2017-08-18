package com.linkedin.datastream.server;

import com.linkedin.datastream.server.api.security.Authorizer;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import kafka.admin.RackAwareMode;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.DummyBootstrapConnector;
import com.linkedin.datastream.connectors.DummyBootstrapConnectorFactory;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.connectors.DummyConnectorFactory;
import com.linkedin.datastream.connectors.file.FileConnector;
import com.linkedin.datastream.connectors.file.FileConnectorFactory;
import com.linkedin.datastream.kafka.EmbeddedZookeeperKafkaCluster;
import com.linkedin.datastream.kafka.KafkaDestination;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.assignment.BroadcastStrategyFactory;
import com.linkedin.datastream.server.assignment.LoadbalancingStrategyFactory;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.KafkaTestUtils;
import com.linkedin.datastream.testutil.TestUtils;

import static com.linkedin.datastream.server.DatastreamServer.*;
import static org.mockito.Mockito.*;


@Test(singleThreaded = true)
public class TestDatastreamServer {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamServer.class.getName());

  public static final String LOAD_BALANCING_STRATEGY_FACTORY = LoadbalancingStrategyFactory.class.getTypeName();
  public static final String BROADCAST_STRATEGY_FACTORY = BroadcastStrategyFactory.class.getTypeName();
  public static final String DUMMY_CONNECTOR = DummyConnector.CONNECTOR_TYPE;
  public static final String DUMMY_BOOTSTRAP_CONNECTOR = DummyBootstrapConnector.CONNECTOR_NAME;
  public static final String FILE_CONNECTOR = FileConnector.CONNECTOR_NAME;

  private EmbeddedDatastreamCluster _datastreamCluster;

  public static EmbeddedDatastreamCluster initializeTestDatastreamServerWithBootstrap() throws Exception {
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(DUMMY_CONNECTOR, getDummyConnectorProperties(true));
    connectorProperties.put(DUMMY_BOOTSTRAP_CONNECTOR, getBootstrapConnectorProperties());
    return EmbeddedDatastreamCluster.newTestDatastreamCluster(new EmbeddedZookeeperKafkaCluster(), connectorProperties,
        new Properties());
  }

  @AfterMethod
  public void cleanup() {
    if (_datastreamCluster != null) {
      _datastreamCluster.shutdown();
    }
  }

  private static Properties getBootstrapConnectorProperties() {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY, BROADCAST_STRATEGY_FACTORY);
    props.put(DatastreamServer.CONFIG_FACTORY_CLASS_NAME, DummyBootstrapConnectorFactory.class.getTypeName());
    return props;
  }

  public static EmbeddedDatastreamCluster initializeTestDatastreamServerWithDummyConnector(Properties override)
      throws Exception {
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(DUMMY_CONNECTOR, getDummyConnectorProperties(false));
    EmbeddedDatastreamCluster datastreamKafkaCluster =
        EmbeddedDatastreamCluster.newTestDatastreamCluster(new EmbeddedZookeeperKafkaCluster(), connectorProperties,
            override);
    return datastreamKafkaCluster;
  }

  private static Properties getDummyConnectorProperties(boolean bootstrap) {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY, BROADCAST_STRATEGY_FACTORY);
    props.put(DatastreamServer.CONFIG_FACTORY_CLASS_NAME, DummyConnectorFactory.class.getTypeName());
    if (bootstrap) {
      props.put(DatastreamServer.CONFIG_CONNECTOR_BOOTSTRAP_TYPE, DUMMY_BOOTSTRAP_CONNECTOR);
    }
    props.put("dummyProperty", "dummyValue");
    return props;
  }

  private EmbeddedDatastreamCluster initializeTestDatastreamServerWithFileConnector(int numServers, String strategy)
      throws IOException, DatastreamException {
    return initializeTestDatastreamServerWithFileConnector(numServers, strategy, 1);
  }

  private EmbeddedDatastreamCluster initializeTestDatastreamServerWithFileConnector(int numServers, String strategy,
      int numDestinationPartitions) throws IOException, DatastreamException {
    Map<String, Properties> connectorProperties = new HashMap<>();
    connectorProperties.put(FILE_CONNECTOR, getTestConnectorProperties(strategy));
    connectorProperties.get(FILE_CONNECTOR)
        .put(FileConnector.CFG_NUM_PARTITIONS, String.valueOf(numDestinationPartitions));
    Properties override = new Properties();
    EmbeddedDatastreamCluster datastreamKafkaCluster =
        EmbeddedDatastreamCluster.newTestDatastreamCluster(new EmbeddedZookeeperKafkaCluster(), connectorProperties,
            override, numServers, null);
    return datastreamKafkaCluster;
  }

  @Test
  public void testDatastreamServerBasics() throws Exception {
    initializeTestDatastreamServerWithDummyConnector(null);
    initializeTestDatastreamServerWithBootstrap();
    _datastreamCluster = initializeTestDatastreamServerWithFileConnector(2, BROADCAST_STRATEGY_FACTORY);
  }

  @Test
  public void testDatastreamServerAuthorization() throws Exception {
    Properties props = new Properties();
    String authzName = "TestAuthz";
    props.put(CONFIG_CONNECTOR_PREFIX + DUMMY_CONNECTOR + "." + CONFIG_CONNECTOR_AUTHORIZER_NAME, authzName);
    _datastreamCluster = initializeTestDatastreamServerWithDummyConnector(props);
    _datastreamCluster.startup();

    Authorizer authz = mock(Authorizer.class);
    when(authz.authorize(anyObject(), anyObject(), anyObject())).thenReturn(true);
    _datastreamCluster.getPrimaryDatastreamServer().getCoordinator().addAuthorizer(authzName, authz);

    Datastream stream = DatastreamTestUtils.createDatastream(DUMMY_CONNECTOR, "A", DummyConnector.VALID_DUMMY_SOURCE);
    DatastreamRestClient restClient = _datastreamCluster.createDatastreamRestClient();
    restClient.createDatastream(stream);

    verify(authz, times(1)).authorize(anyObject(), anyObject(), anyObject());
  }

  @Test
  public void testCreateTwoDatastreamOfFileConnectorProduceEventsReceiveEvents() throws Exception {
    _datastreamCluster = initializeTestDatastreamServerWithFileConnector(1, BROADCAST_STRATEGY_FACTORY, 1);
    int totalEvents = 10;
    _datastreamCluster.startup();
    Path tempFile1 = Files.createTempFile("testFile1", "");
    String fileName1 = tempFile1.toAbsolutePath().toString();
    Datastream fileDatastream1 = createFileDatastream(fileName1, 1);
    Assert.assertEquals((int) fileDatastream1.getDestination().getPartitions(), 1);

    Collection<String> eventsWritten1 = TestUtils.generateStrings(totalEvents);
    FileUtils.writeLines(new File(fileName1), eventsWritten1);

    Collection<String> eventsReceived1 = readFileDatastreamEvents(fileDatastream1, totalEvents);

    LOG.info("Events Received " + eventsReceived1);
    LOG.info("Events Written to file " + eventsWritten1);

    Assert.assertTrue(eventsReceived1.containsAll(eventsWritten1));

    // Test with the second datastream
    Path tempFile2 = Files.createTempFile("testFile2", "");
    String fileName2 = tempFile2.toAbsolutePath().toString();
    Datastream fileDatastream2 = createFileDatastream(fileName2, 1);

    Collection<String> eventsWritten2 = TestUtils.generateStrings(totalEvents);
    FileUtils.writeLines(new File(fileName2), eventsWritten2);

    Collection<String> eventsReceived2 = readFileDatastreamEvents(fileDatastream2, totalEvents);

    LOG.info("Events Received " + eventsReceived2);
    LOG.info("Events Written to file " + eventsWritten2);

    Assert.assertTrue(eventsReceived2.containsAll(eventsWritten2));

    _datastreamCluster.shutdown();
  }

  @Test
  public void testUserManagedDestination() throws Exception {
    int numberOfPartitions = 2;
    String destinationTopic = "userManaged_" + UUID.randomUUID().toString();
    _datastreamCluster =
        initializeTestDatastreamServerWithFileConnector(1, BROADCAST_STRATEGY_FACTORY, numberOfPartitions);
    int totalEvents = 10;
    _datastreamCluster.startup();

    Path tempFile1 = Files.createTempFile("testFile1", "");
    String fileName1 = tempFile1.toAbsolutePath().toString();

    ZkClient zkClient = new ZkClient(_datastreamCluster.getZkConnection());
    ZkConnection zkConnection = new ZkConnection(_datastreamCluster.getZkConnection());
    ZkUtils zkUtils = new ZkUtils(zkClient, zkConnection, false);
    AdminUtils.createTopic(zkUtils, destinationTopic, numberOfPartitions, 1, new Properties(), RackAwareMode.Disabled$.MODULE$);
    KafkaTestUtils.ensureTopicIsReady(_datastreamCluster.getKafkaCluster().getBrokers(), destinationTopic, numberOfPartitions);

    Datastream fileDatastream1 = createFileDatastream(fileName1, destinationTopic, 2);
    Assert.assertEquals((int) fileDatastream1.getDestination().getPartitions(), 2);

    Collection<String> eventsWritten1 = TestUtils.generateStrings(totalEvents);
    FileUtils.writeLines(new File(fileName1), eventsWritten1);

    Collection<String> eventsReceived1 = readFileDatastreamEvents(fileDatastream1, 0, 4);
    Collection<String> eventsReceived2 = readFileDatastreamEvents(fileDatastream1, 1, 6);
    eventsReceived1.addAll(eventsReceived2);

    LOG.info("Events Received " + eventsReceived1);
    LOG.info("Events Written to file " + eventsWritten1);

    Assert.assertTrue(eventsReceived1.containsAll(eventsWritten1));
  }

  @Test
  public void testDeleteDatastreamAndRecreateDatastream() throws Exception {
    _datastreamCluster = initializeTestDatastreamServerWithFileConnector(1, BROADCAST_STRATEGY_FACTORY, 1);
    int totalEvents = 10;
    _datastreamCluster.startup();

    // There is a bug where when we delete the only remaining datastream
    // We are not notifying the connectors about the delete. This is an inherent problem with the existing API
    // because we notify only when there any assignments to the connector.
    Path dummyFile1 = Files.createTempFile("dummyFile", "");
    String dummyFileName = dummyFile1.toAbsolutePath().toString();

    Datastream dummyDatastream = createFileDatastream(dummyFileName, 1);

    Path tempFile1 = Files.createTempFile("testFile1", "");
    String fileName1 = tempFile1.toAbsolutePath().toString();

    LOG.info("Creating the file datastream " + fileName1);
    Datastream fileDatastream1 = createFileDatastream(fileName1, 1);

    LOG.info("Writing events to the file");
    Collection<String> eventsWritten1 = TestUtils.generateStrings(totalEvents);
    FileUtils.writeLines(new File(fileName1), eventsWritten1);

    LOG.info("Reading events from file datastream " + fileDatastream1.getName());
    Collection<String> eventsReceived1 = readFileDatastreamEvents(fileDatastream1, totalEvents);

    LOG.info("Events Received " + eventsReceived1);
    LOG.info("Events Written to file " + eventsWritten1);

    Assert.assertTrue(eventsReceived1.containsAll(eventsWritten1));

    LOG.info("Deleting the datastream " + fileDatastream1.getName());
    DatastreamRestClient restClient = _datastreamCluster.createDatastreamRestClient();
    restClient.deleteDatastream(fileDatastream1.getName());

    // Adding a second sleep so that the new datastream will have a unique destination.
    Thread.sleep(Duration.ofSeconds(1).toMillis());

    LOG.info("Creating the datastream after deletion");
    Datastream fileDatastream2 = createFileDatastream(fileName1, 1);

    LOG.info("Destination for first datastream " + fileDatastream1.getDestination().toString());
    LOG.info("Destination for second datastream " + fileDatastream2.getDestination().toString());

    Assert.assertNotEquals(fileDatastream1.getDestination().getConnectionString(),
        fileDatastream2.getDestination().getConnectionString());

    LOG.info("Writing events to the file " + fileName1);
    eventsWritten1 = TestUtils.generateStrings(totalEvents);
    FileUtils.writeLines(new File(fileName1), eventsWritten1);

    LOG.info("Reading events from file datastream " + fileDatastream2.getName());
    eventsReceived1 = readFileDatastreamEvents(fileDatastream2, totalEvents);

    LOG.info("Events Received " + eventsReceived1);
    LOG.info("Events Written to file " + eventsWritten1);

    Assert.assertTrue(eventsReceived1.containsAll(eventsWritten1));

    Assert.assertNotNull(DynamicMetricsManager.getInstance().getMetric("FileConnector.numDatastreams"));
    Assert.assertNotNull(DynamicMetricsManager.getInstance().getMetric("FileConnector.numDatastreamTasks"));

    _datastreamCluster.shutdown();
  }

  @Test
  public void testNodeDownOneDatastreamSimpleStrategy() throws Exception {
    _datastreamCluster = initializeTestDatastreamServerWithFileConnector(2, LOAD_BALANCING_STRATEGY_FACTORY);
    _datastreamCluster.startup();

    List<DatastreamServer> servers = _datastreamCluster.getAllDatastreamServers();
    Assert.assertEquals(servers.size(), 2);
    Assert.assertNotNull(servers.get(0));
    Assert.assertNotNull(servers.get(1));
    DatastreamServer server1 = servers.get(0);
    DatastreamServer server2 = servers.get(1);

    Path tempFile1 = Files.createTempFile("testFile1", "");
    String fileName1 = tempFile1.toAbsolutePath().toString();

    Datastream fileDatastream1 = createFileDatastream(fileName1, 1);
    int totalEvents = 10;
    List<String> eventsWritten1 = TestUtils.generateStrings(totalEvents);

    // Write some events and make sure DMS properly produces them
    FileUtils.writeLines(new File(fileName1), eventsWritten1);

    List<String> eventsReceived1 = readFileDatastreamEvents(fileDatastream1, totalEvents);

    LOG.info("Events Received " + eventsReceived1);
    LOG.info("Events Written to file " + eventsWritten1);

    Assert.assertTrue(eventsReceived1.containsAll(eventsWritten1));

    // Ensure 1st instance was assigned the task
    String cluster =
        _datastreamCluster.getDatastreamServerProperties().get(0).getProperty(DatastreamServer.CONFIG_CLUSTER_NAME);
    ZkClient zkclient = new ZkClient(_datastreamCluster.getZkConnection());
    String instance = server1.getCoordinator().getInstanceName();
    String assignmentPath = KeyBuilder.instanceAssignments(cluster, instance);
    List<String> assignments = zkclient.getChildren(assignmentPath);
    Assert.assertEquals(assignments.size(), 1);

    LOG.info("Shutting down the server0.");
    // Stop 1st instance and wait until its ZK node is gone
    _datastreamCluster.shutdownServer(0);
    String instancesPath = KeyBuilder.liveInstances(cluster);
    Assert.assertTrue(PollUtils.poll(() -> zkclient.getChildren(instancesPath).size() == 1, 100, 5000));

    // Ensure 2nd instance took over the task
    instance = server2.getCoordinator().getInstanceName();
    assignmentPath = KeyBuilder.instanceAssignments(cluster, instance);
    Assert.assertTrue(PollUtils.poll((path) -> zkclient.getChildren(path).size() == 1, 100, 10000, assignmentPath));

    // Wait 3 seconds to allow the connectors to stop the handler and flush the checkpoints
    // Automatic flush period is 1 second by default.
    Thread.sleep(3000);

    // Ensure 2nd instance can read all
    List<String> eventsWritten2 = TestUtils.generateStrings(totalEvents);

    // Append the lines to test checkpoint functionality where 2nd instance should resume from
    // the previous saved checkpoint by the 1st instance before it died.
    FileUtils.writeLines(new File(fileName1), eventsWritten2, true /* append */);

    // Read twice as many events (eventsWritten1 + eventsWritten2) because
    // KafkaTestUtils.readTopic always seeks to the beginning of the topic.
    List<String> eventsReceived2 = readFileDatastreamEvents(fileDatastream1, totalEvents * 2);

    LOG.info("Events Received " + eventsReceived2);
    LOG.info("First set of events Written to file " + eventsWritten1);
    LOG.info("Second set of events Written to file " + eventsWritten2);

    // If no duplicate events were produced eventsReceived2 should equal eventsWritten1 + eventsWritten2
    // because KafkaTestUtils.readTopic always seeks to the beginning of the topic.
    Assert.assertTrue(eventsReceived2.containsAll(eventsWritten2));
  }

  @Test
  public void testNodeDownOneDatastreamBroadcastStrategy() throws Exception {
    _datastreamCluster = initializeTestDatastreamServerWithFileConnector(2, BROADCAST_STRATEGY_FACTORY);
    _datastreamCluster.startup();

    List<DatastreamServer> servers = _datastreamCluster.getAllDatastreamServers();
    Assert.assertEquals(servers.size(), 2);
    Assert.assertNotNull(servers.get(0));
    Assert.assertNotNull(servers.get(1));
    DatastreamServer server1 = servers.get(0);
    DatastreamServer server2 = servers.get(1);

    Path tempFile1 = Files.createTempFile("testFile1", "");
    String fileName1 = tempFile1.toAbsolutePath().toString();

    Datastream fileDatastream1 = createFileDatastream(fileName1, 1);
    int totalEvents = 10;
    List<String> eventsWritten1 = TestUtils.generateStrings(totalEvents);

    // Start with a few events and make sure DMS properly produces them
    FileUtils.writeLines(new File(fileName1), eventsWritten1);

    List<String> eventsReceived1 = readFileDatastreamEvents(fileDatastream1, totalEvents * 2);

    LOG.info("Events Received " + eventsReceived1);
    LOG.info("Events Written to file " + eventsWritten1);

    // Expect two copies of eventsWritten1 given the two instances and BROADCAST strategy
    Map<String, Integer> countMap = new HashMap<>();
    eventsWritten1.forEach(ev -> countMap.put(ev, 2));
    eventsReceived1.forEach(ev -> countMap.put(ev, countMap.getOrDefault(ev, 0) - 1));
    countMap.forEach((k, v) -> Assert.assertEquals(v, (Integer) 0, "incorrect number of " + k + " is read"));

    // Ensure both instances were assigned the task
    String cluster =
        _datastreamCluster.getDatastreamServerProperties().get(0).getProperty(DatastreamServer.CONFIG_CLUSTER_NAME);
    ZkClient zkclient = new ZkClient(_datastreamCluster.getZkConnection());
    String instance = server1.getCoordinator().getInstanceName();
    String assignmentPath = KeyBuilder.instanceAssignments(cluster, instance);
    List<String> assignments = zkclient.getChildren(assignmentPath);
    Assert.assertEquals(assignments.size(), 1);
    instance = server2.getCoordinator().getInstanceName();
    assignmentPath = KeyBuilder.instanceAssignments(cluster, instance);
    assignments = zkclient.getChildren(assignmentPath);
    Assert.assertEquals(assignments.size(), 1);

    // Stop 1st instance and wait until its ZK node is gone
    _datastreamCluster.shutdownServer(0);
    String instancesPath = KeyBuilder.liveInstances(cluster);
    Assert.assertTrue(PollUtils.poll(() -> zkclient.getChildren(instancesPath).size() == 1, 100, 5000));

    // Ensure 2nd instance still has the task
    instance = server2.getCoordinator().getInstanceName();
    assignmentPath = KeyBuilder.instanceAssignments(cluster, instance);
    Assert.assertTrue(PollUtils.poll((path) -> zkclient.getChildren(path).size() == 1, 100, 10000, assignmentPath));

    // Wait 3 seconds to allow the connectors to stop the handler and flush the checkpoints
    // Automatic flush period is 1 second by default.
    Thread.sleep(3000);

    // Ensure 2nd instance can still produce events
    List<String> eventsWritten2 = TestUtils.generateStrings(totalEvents);

    // Caveat: MUST use appendLines otherwise FileConnector somehow cannot
    // see the newly written lines. This might be due to writeLines overwrites
    // the file. Checking the file creation time does not work because the
    // creation timestamp does not change after writeLines().
    FileUtils.writeLines(new File(fileName1), eventsWritten2, true /* append */);

    // Read three times as many events (eventsWritten1 * 2 + eventsWritten2) because
    // KafkaTestUtils.readTopic always seeks to the beginning of the topic.
    List<String> eventsReceived2 = readFileDatastreamEvents(fileDatastream1, totalEvents * 3);

    LOG.info("Events Received " + eventsReceived2);
    LOG.info("Events Written to file " + eventsWritten2);

    // Expect to see one copy of eventsWritten2 in eventsReceived2
    Map<String, Integer> countMap2 = new HashMap<>();
    eventsWritten2.forEach((ev) -> countMap2.put(ev, 1));
    eventsReceived2.forEach((ev) -> {
      if (countMap2.containsKey(ev)) {
        countMap2.put(ev, countMap2.get(ev) - 1);
      }
    });
    countMap2.forEach((k, v) -> Assert.assertEquals(v, (Integer) 0, "incorrect number of " + k + " is read"));
  }

  // Test is flaky, Need to deflake before enabling it.
  // This test doesn't fail often, Need to run this quite a few times before enabling it
  @Test(enabled = false)
  public void testNodeUpRebalanceTwoDatastreamsSimpleStrategy() throws Exception {
    _datastreamCluster = initializeTestDatastreamServerWithFileConnector(2, LOAD_BALANCING_STRATEGY_FACTORY);
    _datastreamCluster.startupServer(0);

    List<DatastreamServer> servers = _datastreamCluster.getAllDatastreamServers();
    Assert.assertEquals(servers.size(), 2);
    Assert.assertNotNull(servers.get(0));
    DatastreamServer server1 = servers.get(0);

    Path tempFile1 = Files.createTempFile("testFile1", "");
    String fileName1 = tempFile1.toAbsolutePath().toString();

    Path tempFile2 = Files.createTempFile("testFile2", "");
    String fileName2 = tempFile2.toAbsolutePath().toString();

    Datastream fileDatastream1 = createFileDatastream(fileName1, 1);
    Datastream fileDatastream2 = createFileDatastream(fileName2, 1);

    int totalEvents = 10;
    List<String> eventsWritten1 = TestUtils.generateStrings(totalEvents);
    List<String> eventsWritten2 = TestUtils.generateStrings(totalEvents);

    FileUtils.writeLines(new File(fileName1), eventsWritten1);
    FileUtils.writeLines(new File(fileName2), eventsWritten2);

    List<String> eventsReceived1 = readFileDatastreamEvents(fileDatastream1, totalEvents);
    List<String> eventsReceived2 = readFileDatastreamEvents(fileDatastream2, totalEvents);

    LOG.info("(1) Events Received " + eventsReceived1);
    LOG.info("(1) Events Written to file " + eventsWritten1);
    LOG.info("(2) Events Received " + eventsReceived2);
    LOG.info("(2) Events Written to file " + eventsWritten2);

    Assert.assertTrue(eventsReceived1.containsAll(eventsWritten1));
    Assert.assertTrue(eventsReceived2.containsAll(eventsWritten2));

    // Ensure 1st instance was assigned both tasks
    String cluster =
        _datastreamCluster.getDatastreamServerProperties().get(0).getProperty(DatastreamServer.CONFIG_CLUSTER_NAME);
    ZkClient zkclient = new ZkClient(_datastreamCluster.getZkConnection());
    String instance1 = server1.getCoordinator().getInstanceName();
    String assignmentPath = KeyBuilder.instanceAssignments(cluster, instance1);
    List<String> assignments = zkclient.getChildren(assignmentPath);
    Assert.assertEquals(assignments.size(), 2);

    // Start 2nd instance and wait until it shows up in ZK
    _datastreamCluster.startupServer(1);
    DatastreamServer server2 = servers.get(1);
    Assert.assertNotNull(server2);
    String instancesPath = KeyBuilder.liveInstances(cluster);
    Assert.assertTrue(PollUtils.poll(() -> zkclient.getChildren(instancesPath).size() == 2, 100, 5000));

    // Ensure each instance gets one task
    assignmentPath = KeyBuilder.instanceAssignments(cluster, instance1);
    Assert.assertTrue(PollUtils.poll((path) -> zkclient.getChildren(path).size() == 1, 100, 10000, assignmentPath));
    LOG.info("Instance1 got task: " + zkclient.getChildren(assignmentPath));

    String instance2 = server2.getCoordinator().getInstanceName();
    assignmentPath = KeyBuilder.instanceAssignments(cluster, instance2);
    Assert.assertTrue(PollUtils.poll((path) -> zkclient.getChildren(path).size() == 1, 100, 10000, assignmentPath));
    LOG.info("Instance2 got task: " + zkclient.getChildren(assignmentPath));

    // Wait 3 seconds to allow the connectors to stop the handler and flush the checkpoints
    // Automatic flush period is 1 second by default.
    Thread.sleep(3000);

    eventsWritten1 = TestUtils.generateStrings(totalEvents);
    eventsWritten2 = TestUtils.generateStrings(totalEvents);

    FileUtils.writeLines(new File(fileName1), eventsWritten1, true /* append */);
    FileUtils.writeLines(new File(fileName2), eventsWritten2, true /* append */);

    // Read twice as many events because KafkaTestUtils.readTopic always seeks
    // to the beginning of the topic such that previous events are included
    eventsReceived1 = readFileDatastreamEvents(fileDatastream1, totalEvents * 2);
    eventsReceived2 = readFileDatastreamEvents(fileDatastream2, totalEvents * 2);

    LOG.info("(1-NEW) Events Received " + eventsReceived1);
    LOG.info("(1-NEW) Events Written to file " + eventsWritten1);
    LOG.info("(2-NEW) Events Received " + eventsReceived2);
    LOG.info("(2-NEW) Events Written to file " + eventsWritten2);

    Assert.assertTrue(eventsReceived1.containsAll(eventsWritten1));
    Assert.assertTrue(eventsReceived2.containsAll(eventsWritten2));
  }

  private List<String> readFileDatastreamEvents(Datastream datastream, int totalEvents) throws Exception {
    return readFileDatastreamEvents(datastream, 0, totalEvents);
  }

  private List<String> readFileDatastreamEvents(Datastream datastream, int partition, int totalEvents)
      throws Exception {
    KafkaDestination kafkaDestination = KafkaDestination.parse(datastream.getDestination().getConnectionString());
    final int[] numberOfMessages = {0};
    List<String> eventsReceived = new ArrayList<>();
    KafkaTestUtils.readTopic(kafkaDestination.getTopicName(), partition,
        _datastreamCluster.getKafkaCluster().getBrokers(), (key, value) -> {
          String eventValue = new String(value);
          LOG.info("Read {}th or {} events, Event {} from datastream {}", numberOfMessages[0], totalEvents, eventValue,
              datastream);
          eventsReceived.add(eventValue);
          numberOfMessages[0]++;
          return numberOfMessages[0] < totalEvents;
        });

    return eventsReceived;
  }

  private Datastream createFileDatastream(String fileName, int numPartitions) throws IOException, DatastreamException {
    String topic = Paths.get(fileName).getFileName().toString().replace(".", "_") + System.currentTimeMillis();
    return createFileDatastream(fileName, topic, numPartitions);
  }

  private Datastream createFileDatastream(String fileName, String destinationTopic, int destinationPartitions)
      throws IOException, DatastreamException {
    File testFile = new File(fileName);
    testFile.createNewFile();
    testFile.deleteOnExit();
    Datastream fileDatastream1;
    String kafkaZk = _datastreamCluster.getKafkaCluster().getZkConnection();
    KafkaDestination destination = new KafkaDestination(kafkaZk, destinationTopic, false);
    if (destinationTopic != null) {
      fileDatastream1 = DatastreamTestUtils.createDatastream(FileConnector.CONNECTOR_NAME, "file_" + testFile.getName(),
          testFile.getAbsolutePath(), destinationTopic, destinationPartitions);
    } else {
      fileDatastream1 = DatastreamTestUtils.createDatastream(FileConnector.CONNECTOR_NAME, "file_" + testFile.getName(),
          testFile.getAbsolutePath());
    }

    // DatastreamTestUtils does not properly initialize destination URI
    fileDatastream1.getDestination().setConnectionString(destination.getDestinationURI());

    DatastreamRestClient restClient = _datastreamCluster.createDatastreamRestClient();
    restClient.createDatastream(fileDatastream1);
    return getPopulatedDatastream(restClient, fileDatastream1);
  }

  private Datastream getPopulatedDatastream(DatastreamRestClient restClient, Datastream fileDatastream1) {
    Boolean pollResult = PollUtils.poll(() -> {
      Datastream ds = null;
      ds = restClient.getDatastream(fileDatastream1.getName());
      return ds.hasDestination() && ds.getDestination().hasConnectionString() && !ds.getDestination()
          .getConnectionString()
          .isEmpty();
    }, 500, 60000);

    if (pollResult) {
      return restClient.getDatastream(fileDatastream1.getName());
    } else {
      throw new RuntimeException("Destination was not populated before the timeout");
    }
  }

  private Properties getTestConnectorProperties(String strategy) {
    Properties props = new Properties();
    props.put(DatastreamServer.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY, strategy);
    props.put(DatastreamServer.CONFIG_FACTORY_CLASS_NAME, FileConnectorFactory.class.getTypeName());
    return props;
  }
}
