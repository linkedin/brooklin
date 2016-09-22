package com.linkedin.datastream.server;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DynamicMetricsManager;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.kafka.KafkaTransportProvider;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.server.assignment.LoadbalancingStrategy;
import com.linkedin.datastream.server.dms.DatastreamResources;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;


public class TestCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(TestCoordinator.class);
  private static final String TRANSPORT_FACTORY_CLASS = DummyTransportProviderFactory.class.getTypeName();
  private static final long WAIT_DURATION_FOR_ZK = Duration.ofMinutes(1).toMillis();
  private static final int WAIT_TIMEOUT_MS = 30000;

  EmbeddedZookeeper _embeddedZookeeper;
  String _zkConnectionString;

  static {
    DynamicMetricsManager.createInstance(new MetricRegistry());
  }

  private Coordinator createCoordinator(String zkAddr, String cluster) throws Exception {
    Properties props = new Properties();
    props.put(CoordinatorConfig.CONFIG_CLUSTER, cluster);
    props.put(CoordinatorConfig.CONFIG_ZK_ADDRESS, zkAddr);
    props.put(CoordinatorConfig.CONFIG_ZK_SESSION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_SESSION_TIMEOUT));
    props.put(CoordinatorConfig.CONFIG_ZK_CONNECTION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_CONNECTION_TIMEOUT));
    props.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, TRANSPORT_FACTORY_CLASS);
    ZkClient client = new ZkClient(zkAddr);
    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(client, cluster);
    return new Coordinator(datastreamCache, props);
  }

  @BeforeMethod
  public void setup() throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper();
    _zkConnectionString = _embeddedZookeeper.getConnection();
    _embeddedZookeeper.startup();
  }

  @AfterMethod
  public void teardown() throws IOException {
    _embeddedZookeeper.shutdown();
  }

  public class TestHookConnector implements Connector {
    boolean _isStarted = false;
    String _connectorType = "TestConnector";
    List<DatastreamTask> _tasks = new ArrayList<>();
    String _instance = "";
    String _name;

    public TestHookConnector(String name, String connectorType) {
      _name = name;
      _connectorType = connectorType;
    }

    public TestHookConnector(String connectorType) {
      _connectorType = connectorType;
    }

    public String getName() {
      return _name;
    }

    public List<DatastreamTask> getTasks() {
      LOG.info(_name + ": getTasks. Instance: " + _instance + ", size: " + _tasks.size() + ", tasks: " + _tasks);
      return _tasks;
    }

    @Override
    public void start() {
      _isStarted = true;
      LOG.info("Connector " + _name + " started");
    }

    @Override
    public void stop() {
      _isStarted = false;
    }

    @Override
    public void onAssignmentChange(List<DatastreamTask> tasks) {

      LOG.info("START: onAssignmentChange. Name: " + _name + ", ConnectorType: " + _connectorType
          + ",  Number of assignments: " + tasks.size() + ", tasks: " + tasks);

      _tasks = tasks;
      for (DatastreamTask task : tasks) {
        if (task.getEventProducer() == null) {
          Assert.assertNotNull(task.getEventProducer());
        }
      }

      LOG.info("END: onAssignmentChange");
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) {
    }

    @Override
    public String toString() {
      return "Connector " + _name + ", StatusId: " + _connectorType + ", Instance: " + _instance;
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return null;
    }
  }

  /**
   * testConnectorStateSetAndGet makes sure that the connector can read and write state that
   * is specific to each DatastreamTask.
   *
   * @throws Exception
   */
  // This test is disabled because there are still some issues around saving the state. This should be fixed as part of
  // Scenario #3.
  @Test
  public void testConnectorStateSetAndGet() throws Exception {
    String testCluster = "testConnectorStateSetAndGet";
    String testConectorType = "testConnectorType";

    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);
    Set<String> taskNames = new HashSet<>();
    //
    // create a Connector instance, its sole purpose is to record the number of times
    // the onAssignmentChange() is called, and it will persist this value for each
    // task
    //
    Connector testConnector = new Connector() {
      @Override
      public void start() {
      }

      @Override
      public void stop() {
      }

      @Override
      public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
        // for each instance of assigned DatastreamTask, we keep a state with the key
        // "counter". Every time onAssignmentChange() is called, we increment this counter
        // by one for each assigned task.
        tasks.forEach(task -> {
          String counter = task.getState("counter");
          if (counter == null) {
            task.saveState("counter", "1");
            taskNames.add(task.getDatastreamTaskName());
          } else {
            int c = Integer.parseInt(counter);
            task.saveState("counter", Integer.toString(c + 1));
          }
        });
      }

      @Override
      public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) {
      }

      @Override
      public Map<String, Metric> getMetrics() {
        return null;
      }
    };

    coordinator.addConnector(testConectorType, testConnector, new BroadcastStrategy(), false);
    coordinator.start();
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    //
    // create a new datastream so that the onAssignmentChange() can be called
    //
    String datastreamName1 = "datastream1";
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConectorType, datastreamName1);

    //
    // verify that the counter value for the connector is 1 because the onAssignmentChange
    // should be called once
    //
    PollUtils.poll(() -> taskNames.size() == 1, 500, 30000);
    String name1 = (String) taskNames.toArray()[0];
    String datastream1CounterPath = KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, name1, "counter");
    Assert.assertTrue(PollUtils.poll((path) -> zkClient.exists(path), 500, 30000, datastream1CounterPath));
    Assert.assertEquals(zkClient.readData(datastream1CounterPath), "1");
    //
    // add a second datastream named datastream2
    //
    String datastreamName2 = "datastream2";
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConectorType, datastreamName2);
    PollUtils.poll(() -> taskNames.size() == 2, 500, 30000);
    String name2 = (String) taskNames.toArray()[1];
    String datastream2CounterPath = KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, name2, "counter");
    Assert.assertTrue(PollUtils.poll((path) -> zkClient.exists(path), 500, 30000, datastream2CounterPath));
    //
    // verify that the counter for datastream1 is "2" but the counter for datastream2 is "1"
    //
    //    Assert.assertEquals(zkClient.readData(datastream1CounterPath), "2");
    //    Assert.assertEquals(zkClient.readData(datastream2CounterPath), "1");
    //
    Thread.sleep(1000 * 60);

    //
    // clean up
    //
    zkClient.close();
    coordinator.stop();
  }

  // verify that connector znodes are created as soon as Coordinator instance is started
  @Test
  public void testConnectorZkNodes() throws Exception {
    String testCluster = "testConnectorZkNodes";
    String testConectorType = "testConnectorType";

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConectorType);
    instance1.addConnector(testConectorType, connector1, new BroadcastStrategy(), false);
    instance1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    String znode = KeyBuilder.connector(testCluster, testConectorType);
    Assert.assertTrue(zkClient.exists(znode));

    zkClient.close();
    instance1.stop();
  }

  /**
   * testCoordinationWithBroadcastStrategy is a smoke test, to verify that datastreams created by DSM can be
   * assigned to live instances. The datastreams created by DSM is mocked by directly creating
   * the znodes in zookeeper. The steps involved:
   * <ul>
   *     <li>create a cluster with 1 live instance named instance1, start the live instance</li>
   *     <li>create the first datastream (datastream1) with broadcast strategy, and verify it is assigned to instance1</li>
   *     <li>create a second live instance named instance2 and join the cluster</li>
   *     <li>verify that instance2 is also assigned the same datastream datastream1</li>
   *     <li>create a second datastream (datastream2)</li>
   *     <li>verify that datastream2 is assigned to both instance1 and instance2</li>
   * </ul>
   *
   * @throws Exception
   */
  @Test
  public void testCoordinationWithBroadcastStrategy() throws Exception {
    String testCluster = "testCoordinationSmoke";
    String testConectorType = "testConnectorType";
    String datastreamName1 = "datastream1";

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConectorType);
    instance1.addConnector(testConectorType, connector1, new BroadcastStrategy(), false);
    instance1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create datastream definitions under /testAssignmentBasic/datastream/datastream1
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConectorType, datastreamName1);

    //
    // verify the instance has 1 task assigned: datastream1
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamName1);

    //
    // create a second live instance named instance2 and join the cluster
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConectorType);
    instance2.addConnector(testConectorType, connector2, new BroadcastStrategy(), false);
    instance2.start();

    //
    // verify instance2 has 1 task assigned
    //
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, datastreamName1);

    //
    // create a new datastream definition for the same connector type, /testAssignmentBasic/datastream/datastream2
    //
    String datastreamName2 = "datastream2";
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConectorType, datastreamName2);

    //
    // verify both instance1 and instance2 now have two datastreamtasks assigned
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream1", "datastream2");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1", "datastream2");

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
  }

  private void setDatastreamDestination(Datastream stream) {
    if (stream.getDestination() == null) {
      stream.setDestination(new DatastreamDestination());
      // The connection string here needs to be unique
      String destinationConnectionString = UUID.randomUUID().toString();
      stream.getDestination().setConnectionString(destinationConnectionString);
    }
  }

  @Test
  public void testCoordinationMultipleConnectorTypesForBroadcastStrategy() throws Exception {
    String testCluster = "testCoordinationMultipleConnectors";

    String connectorType1 = "connectorType1";
    String connectorType2 = "connectorType2";

    LOG.info("create two coordinators and two connector instances per coordinator of broadcast strategy");

    //
    // create two live instances, each handle two different types of connectors
    //
    TestHookConnector connector11 = new TestHookConnector("connector11", connectorType1);
    TestHookConnector connector12 = new TestHookConnector("connector12", connectorType2);

    TestHookConnector connector21 = new TestHookConnector("connector21", connectorType1);
    TestHookConnector connector22 = new TestHookConnector("connector22", connectorType2);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    instance1.addConnector(connectorType1, connector11, new BroadcastStrategy(), false);
    instance1.addConnector(connectorType2, connector12, new BroadcastStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    instance2.addConnector(connectorType1, connector21, new BroadcastStrategy(), false);
    instance2.addConnector(connectorType2, connector22, new BroadcastStrategy(), false);
    instance2.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create a datastream of connectorType1");

    //
    // create a new datastream for connectorType1
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType1, "datastream1");

    LOG.info("Verify whether the datastream is assigned to connector instances on both the coordinator");

    //
    // verify both live instances have tasks assigned for connector type 1 only
    //
    assertConnectorAssignment(connector11, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector12.getTasks().isEmpty());

    assertConnectorAssignment(connector21, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector22.getTasks().isEmpty());

    LOG.info("Create a datastream of connectorType2");

    //
    // create a new datastream for connectorType2
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType2, "datastream2");

    LOG.info("Verify the assignment");

    //
    // verify both live instances have tasks assigned for both connector types
    //
    assertConnectorAssignment(connector11, WAIT_TIMEOUT_MS, "datastream1");
    assertConnectorAssignment(connector12, WAIT_TIMEOUT_MS, "datastream2");

    assertConnectorAssignment(connector21, WAIT_TIMEOUT_MS, "datastream1");
    assertConnectorAssignment(connector22, WAIT_TIMEOUT_MS, "datastream2");

    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  //
  // stress test, start multiple coordinator instances at the same time, and make sure that all of them
  // will get a unique instance name
  //
  @Test(enabled = false)
  public void testStressLargeNumberOfLiveInstances() throws Exception {
    int concurrencyLevel = 100;
    String testCluster = "testStressUniqueInstanceNames";
    ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    // the duration of each live instance thread, make sure it is long enough to verify the result
    long duration = WAIT_DURATION_FOR_ZK;

    for (int i = 0; i < concurrencyLevel; i++) {
      Runnable task = () -> {
        // keep the thread alive
        try {
          Coordinator instance = createCoordinator(_zkConnectionString, testCluster);
          instance.start();

          Thread.sleep(duration);
          instance.stop();
        } catch (Exception ex) {
          LOG.error("Failed to launch coordinator", ex);
          Assert.fail();
        }
      };

      executor.execute(task);
    }

    //
    // wait for all instances go online
    //
    Thread.sleep(duration);

    //
    // verify all instances are alive
    //
    List<String> instances = zkClient.getChildren(KeyBuilder.liveInstances(testCluster));

    List<String> sortedList = new ArrayList<>(instances);
    Collections.sort(sortedList);
    LOG.info(String.format("Live instances %s", sortedList));

    Assert.assertEquals(instances.size(), concurrencyLevel, String.format("Live instances %s", sortedList));
    zkClient.close();
  }

  @Test
  public void testStressLargeNumberOfDatastreams() throws Exception {

    int concurrencyLevel = 10;

    String testCluster = "testStressLargeNumberOfDatastreams";
    String testConectorType = "testConnectorType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create two coordinator and two connector instances of broadcast strategy");

    //
    // create 1 live instance and start it
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConectorType);
    instance1.addConnector(testConectorType, connector1, new BroadcastStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);

    TestHookConnector connector2 = new TestHookConnector("connector2", testConectorType);
    instance2.addConnector(testConectorType, connector2, new BroadcastStrategy(), false);
    instance2.start();

    String[] datastreamNames = new String[concurrencyLevel];

    LOG.info("Create 10 datastreams");

    //
    // create large number of datastreams
    //
    for (int i = 0; i < concurrencyLevel; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConectorType, datastreamName + i);
      datastreamNames[i] = datastreamName + i;
    }

    LOG.info("validate whether all the datastreams are assigned to all the instances");

    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, datastreamNames);
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, datastreamNames);

    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  //
  // Test SimpleAssignmentStrategy: if new live instances come online, some tasks
  // will be moved from existing live instance to the new live instance
  //
  @Test
  public void testSimpleAssignmentReassignWithNewInstances() throws Exception {
    String testCluster = "testSimpleAssignmentReassignWithNewInstances";
    String testConnectoryType = "testConnectoryType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Creating the first coordinator and connector instance");
    //
    // create 1 instance
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new LoadbalancingStrategy(), false);
    instance1.start();

    LOG.info("Creating two datastream");

    //
    // create 2 datastreams, [datastream0, datastream1]
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream0");
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream1");

    //
    // verify both datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream1");

    LOG.info("Creating the second coordinator and connector instance");

    //
    // add a new live instance instance2
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new LoadbalancingStrategy(), false);
    instance2.start();



    //
    // verify new assignment. instance1 : [datastream0], instance2: [datastream1]
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1");

    LOG.info("Creating the third coordinator and connector instance");

    //
    // add instance3
    //
    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector("connector3", testConnectoryType);
    instance3.addConnector(testConnectoryType, connector3, new LoadbalancingStrategy(), false);
    instance3.start();

    //
    // verify assignment didn't change
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1");
    Assert.assertTrue(connector3.getTasks().isEmpty());

    //
    // clean up
    //
    zkClient.close();
    instance1.stop();
    instance2.stop();
    instance3.stop();
  }

  //
  // Test for SimpleAssignmentStrategy
  // Verify that when instance dies, the assigned tasks will be re-assigned to remaining live instances
  //
  @Test
  public void testSimpleAssignmentReassignAfterDeath() throws Exception {
    String testCluster = "testSimpleAssignmentReassignAfterDeath";
    String testConnectoryType = "testConnectoryType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create two coordinators and two connector instances");

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new LoadbalancingStrategy(), false);
    instance1.start();

    // make sure the instance2 can be taken offline cleanly with session expiration
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new LoadbalancingStrategy(), false);
    instance2.start();

    LOG.info("Create four datastreams");

    //
    // create 4 datastreams, [datastream0, datastream1, datatream2, datastream3]
    //
    for (int i = 0; i < 4; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    LOG.info("Verify that the datastrems are assigned across two connectors");

    waitTillAssignmentIsComplete(connector1, connector2, 4, WAIT_TIMEOUT_MS);
    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, WAIT_DURATION_FOR_ZK, "datastream0", "datastream2");
    assertConnectorAssignment(connector2, WAIT_DURATION_FOR_ZK, "datastream1", "datastream3");

    LOG.info("Tasks assigned to instance1: " + connector1.getTasks().toString());
    LOG.info("Tasks assigned to instance2: " + connector2.getTasks().toString());

    List<DatastreamTask> tasks1 = new ArrayList<>(connector1.getTasks());
    tasks1.addAll(connector2.getTasks());
    Collections.sort(tasks1, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Take the instance2 offline");

    //
    // take instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    LOG.info("verify that the four datastreams are assigned to the instance1");

    //
    // verify all 4 datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream1", "datastream2", "datastream3");

    // Make sure strategy reused all tasks as opposed to creating new ones
    List<DatastreamTask> tasks2 = new ArrayList<>(connector1.getTasks());
    Collections.sort(tasks2, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Tasks1: " + tasks1.toString());
    LOG.info("Tasks2: " + tasks2.toString());

    Assert.assertEquals(tasks1, tasks2);

    // Verify dead instance assignments have been removed
    // Assert.assertTrue(!zkClient.exists(KeyBuilder.instanceAssignments(testCluster, instance2.getInstanceName())));

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
  }



  @Test
  public void testBroadcastAssignmentReassignAfterDeath() throws Exception {
    String testCluster = "testBroadcastAssignmentReassignAfterDeath";
    String testConnectoryType = "testConnectoryType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Creating two coordinator and two connector instances");

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new BroadcastStrategy(), false);
    instance1.start();

    // make sure the instance2 can be taken offline cleanly with session expiration
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new BroadcastStrategy(), false);
    instance2.start();

    LOG.info("Create two datastreams");

    //
    // create 2 datastreams, [datastream0, datastream1]
    //
    for (int i = 0; i < 2; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    LOG.info("Validate the broadcast assignment");

    //
    // verify assignment, instance1: [datastream0, datastream1], instance2:[datastream0, datastream1]
    //
    assertConnectorAssignment(connector1, WAIT_DURATION_FOR_ZK, "datastream0", "datastream1");
    assertConnectorAssignment(connector2, WAIT_DURATION_FOR_ZK, "datastream0", "datastream1");

    List<DatastreamTask> tasks2 = new ArrayList<>(connector2.getTasks());

    LOG.info("Take the instance2 offline");

    //
    // take instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    LOG.info("Verify whether the live instance assignment has been removed.");

    // Verify dead instance assignments have been removed
    String instancePath = KeyBuilder.instanceAssignments(testCluster, instance2.getInstanceName());
    Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(instancePath), 200, WAIT_TIMEOUT_MS));

    LOG.info("Verify instance1 still has two datastreams");

    //
    // verify instance1 still has 2 datastreams
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream1");

    // Make sure Coordinator has removed deprecated connector tasks of instance2
    for (DatastreamTask task : tasks2) {
      String path = KeyBuilder.connectorTask(testCluster, task.getConnectorType(), task.getDatastreamTaskName());
      LOG.info("Checking whether the path doesn't exist anymore: " + path);
      Assert.assertTrue(PollUtils.poll(() -> !zkClient.exists(path), 200, WAIT_TIMEOUT_MS));
    }

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
  }

  //
  // this case tests the scenario when the leader of the cluster dies, and make sure
  // the assignment will be taken over by the new leader.
  //
  @Test
  public void testSimpleAssignmentReassignAfterLeaderDeath() throws Exception {
    String testCluster = "testSimpleAssignmentReassignAfterLeaderDeath";
    String testConnectoryType = "testConnectoryType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Creating three coordinator and connector instances ");
    //
    // setup a cluster with 3 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new LoadbalancingStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new LoadbalancingStrategy(), false);
    instance2.start();

    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector("connector3", testConnectoryType);
    instance3.addConnector(testConnectoryType, connector3, new LoadbalancingStrategy(), false);
    instance3.start();

    LOG.info("Creating six datastreams");
    //
    // create 6 datastreams, [datastream0, ..., datastream5]
    //
    for (int i = 0; i < 6; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    LOG.info("Verify whether the six datastreams are assigned to the three connector instances");

    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, WAIT_DURATION_FOR_ZK, "datastream0", "datastream3");
    assertConnectorAssignment(connector2, WAIT_DURATION_FOR_ZK, "datastream1", "datastream4");
    assertConnectorAssignment(connector3, WAIT_DURATION_FOR_ZK, "datastream2", "datastream5");

    List<DatastreamTask> tasks1 = new ArrayList<>(connector1.getTasks());
    tasks1.addAll(connector2.getTasks());
    tasks1.addAll(connector3.getTasks());
    Collections.sort(tasks1, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Stop the instance1 and delete the live instance");

    //
    // take current leader instance1 offline
    //
    instance1.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance1);

    LOG.info("Verify that the 6 datastreams are assigned to the remaining two instances");

    //
    // verify all 6 datastreams are assigned to instance2 and instance3
    //
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream0", "datastream2", "datastream4");
    assertConnectorAssignment(connector3, WAIT_TIMEOUT_MS, "datastream1", "datastream3", "datastream5");

    LOG.info("Stop the instance2 and delete the live instance");

    //
    // take current leader instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    LOG.info("Verify that the 6 datastreams are assigned to remaining one instance");

    //
    // verify all tasks assigned to instance3
    assertConnectorAssignment(connector3, WAIT_TIMEOUT_MS, "datastream0", "datastream2", "datastream4", "datastream1",
        "datastream3", "datastream5");

    LOG.info("Make sure strategy reused all the tasks as opposed to creating new ones");

    // Make sure strategy reused all tasks as opposed to creating new ones
    List<DatastreamTask> tasks2 = new ArrayList<>(connector3.getTasks());
    Collections.sort(tasks2, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    LOG.info("Tasks1: " + tasks1.toString());
    LOG.info("Tasks2: " + tasks2.toString());

    Assert.assertEquals(tasks1, tasks2);

    // Verify dead instance assignments have been removed
    Assert.assertTrue(!zkClient.exists(KeyBuilder.instanceAssignments(testCluster, instance1.getInstanceName())));
    Assert.assertTrue(!zkClient.exists(KeyBuilder.instanceAssignments(testCluster, instance2.getInstanceName())));

    //
    // clean up
    //
    instance3.stop();
    zkClient.close();
  }

  //
  // this test covers the scenario when multiple instances die at the same time
  //
  @Test
  public void testMultipleInstanceDeath() throws Exception {
    String testCluster = "testMultipleInstanceDeath";
    String testConnectoryType = "testConnectoryType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create four instances");
    //
    // create a list of instances
    //
    int count = 4;
    Coordinator[] coordinators = new Coordinator[count];
    TestHookConnector[] connectors = new TestHookConnector[count];
    for (int i = 0; i < count; i++) {
      coordinators[i] = createCoordinator(_zkConnectionString, testCluster);
      connectors[i] = new TestHookConnector("connector" + i, testConnectoryType);
      coordinators[i].addConnector(testConnectoryType, connectors[i], new LoadbalancingStrategy(), false);
      coordinators[i].start();
    }

    LOG.info("Create four datastreams");

    //
    // create 1 datastream per instance
    //
    for (int i = 0; i < count; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    LOG.info("Validate whether the four datastreams are assigned to four instances");

    //
    // wait until the last instance was assigned the last datastream, by now all datastream should be assigned
    //
    assertConnectorAssignment(connectors[count - 1], WAIT_TIMEOUT_MS, "datastream" + (count - 1));

    LOG.info("kill three instances except for the leader");

    //
    // kill all instances except the current leader
    //
    for (int i = 1; i < count; i++) {
      coordinators[i].stop();
      deleteLiveInstanceNode(zkClient, testCluster, coordinators[i]);
    }

    LOG.info("Check whether all the instances are assigned to the only remaining instance.");

    //
    // validate all datastream tasks are assigned to the leader now
    //
    String[] assignment = new String[count];
    for (int i = 0; i < count; i++) {
      assignment[i] = datastreamName + i;
    }
    assertConnectorAssignment(connectors[0], WAIT_TIMEOUT_MS, assignment);

    //
    // clean up
    //
    coordinators[0].stop();
    zkClient.close();
  }

  //
  // Test SimpleAssignmentStrategy, verify that the assignment is predictable no matter what the datastreams
  // are. This is because the assignment strategy will sort the datastreams by names. If a new datastream
  // has a smaller lexicographical order, it will be assigned to an instance with smaller lexicographical order.
  // Put it in another word, this is how Kafka consumer rebalancing works.
  //
  @Test
  public void testSimpleAssignmentRebalancing() throws Exception {
    String testCluster = "testSimpleAssignmentRebalancing";
    String testConnectoryType = "testConnectoryType";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info("Create two coordinators and connector instances");

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector("connector1", testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new LoadbalancingStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector("connector2", testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new LoadbalancingStrategy(), false);
    instance2.start();

    LOG.info("Create two datastreams.");

    //
    // create 2 datastreams [datastream1, datastream2]
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream1");
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream2");

    LOG.info("verify that two datastreams are assigned to two instances");

    //
    // verify assignment instance1: [datastream1], instance2:[datastream2]
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream1");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream2");

    LOG.info("create a third datastream but which is smaller than earlier two");
    //
    // create 1 new datastream "datastream0", which has the smallest lexicographical order
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream0");

    LOG.info("Verify that the new datastream is assigned to first instance.");

    //
    // verify assignment instance1:[datastream0, datastream2], instance2:[datastream1]
    //
    assertConnectorAssignment(connector1, WAIT_TIMEOUT_MS, "datastream0", "datastream2");
    assertConnectorAssignment(connector2, WAIT_TIMEOUT_MS, "datastream1");

    //
    // clean up
    //
    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  //
  // Verify each connector type has its assignment strategy and it is executed independently
  // That is, the assignment of one connector type will not affect the assignment of the
  // other type, even though the assignment strategies are different. In this test case,
  // we have two connectors for each instance, and they are using different assignment
  // strategies, BroadcastStrategy and SimpleStrategy respectively.
  //
  @Test
  public void testSimpleAssignmentStrategyIndependent() throws Exception {
    String testCluster = "testSimpleAssignmentStrategy";
    String connectoryType1 = "ConnectoryType1";
    String connectoryType2 = "ConnectoryType2";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    LOG.info(
        "Create two coordinator with two connctor types (one simple and one broadcast) in each and create a connector instance"
            + " of each connector type per coordinator");

    //
    // setup a cluster with 2 live instances with simple assignment strategy,
    // each has two connectors
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1a = new TestHookConnector("connector1a", connectoryType1);
    TestHookConnector connector1b = new TestHookConnector("connector1b", connectoryType2);
    instance1.addConnector(connectoryType1, connector1a, new LoadbalancingStrategy(), false);
    instance1.addConnector(connectoryType2, connector1b, new BroadcastStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2a = new TestHookConnector("connector2a", connectoryType1);
    TestHookConnector connector2b = new TestHookConnector("connector2b", connectoryType2);
    instance2.addConnector(connectoryType1, connector2a, new LoadbalancingStrategy(), false);
    instance2.addConnector(connectoryType2, connector2b, new BroadcastStrategy(), false);
    instance2.start();

    LOG.info("Create three datastreams of connectorType1 and three datastreams of connectorType2");

    //
    // create 3 datastreams ["simple0", "simple1", "simple2"] for ConnectoryType1
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType1, "simple0", "simple1",
        "simple2");
    //
    // create 3 datastreams [datastream2, datastream3, datastream4] for ConnectorType2
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType2, "broadcast0", "broadcast1",
        "broadcast2");

    LOG.info("verify that the datastreams are properly assigned based on simple or broadcast");
    //
    // verify assignment: instance1.connector1: [datastream0], connector2:[datastream2, datastream4"]
    // instance2.connector1:[datastream1], connector2:[datastream3]
    //
    assertConnectorAssignment(connector1a, WAIT_TIMEOUT_MS, "simple0", "simple2");
    assertConnectorAssignment(connector1b, WAIT_TIMEOUT_MS, "broadcast0", "broadcast1", "broadcast2");
    assertConnectorAssignment(connector2a, WAIT_TIMEOUT_MS, "simple1");
    assertConnectorAssignment(connector2b, WAIT_TIMEOUT_MS, "broadcast0", "broadcast1", "broadcast2");

    //
    // clean up
    //
    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  class BadConnector implements Connector {
    private int assignmentCount;

    public int getAssignmentCount() {
      return assignmentCount;
    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public void onAssignmentChange(List<DatastreamTask> tasks) {
      ++assignmentCount;
      // throw a fake exception to trigger the error handling
      throw new RuntimeException();
    }

    @Override
    public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
        throws DatastreamValidationException {
    }

    @Override
    public Map<String, Metric> getMetrics() {
      return null;
    }
  }

  @Test
  public void testCoordinatorErrorHandling() throws Exception {
    String testCluster = "testCoordinatorErrorHandling";
    String connectoryType1 = "ConnectoryType1";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    BadConnector connector1 = new BadConnector();
    instance1.addConnector(connectoryType1, connector1, new BroadcastStrategy(), false);
    instance1.start();

    //
    // validate the error nodes has 0 child because onAssignmentChange is not triggered yet
    //
    String errorPath = KeyBuilder.instanceErrors(testCluster, instance1.getInstanceName());
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 0, 500, WAIT_TIMEOUT_MS);

    //
    // create a new datastream, which will trigger the error path
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType1, "datastream0");

    //
    // validate the error nodes now has 1 child
    //
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 1, 500, WAIT_TIMEOUT_MS);

    //
    // create another datastream, and validate the error nodes have 2 children
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType1, "datastream1");
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 2, 500, WAIT_TIMEOUT_MS);

    //
    // create 10 more datastream, and validate the error children is caped at 10
    //
    for (int i = 2; i < 12; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType1, "datastream" + i);
    }
    PollUtils.poll(() -> connector1.getAssignmentCount() == 12, 200, WAIT_TIMEOUT_MS);
    int childrenCount = zkClient.countChildren(errorPath);
    Assert.assertTrue(childrenCount <= 10);

    //
    // clean up
    //
    zkClient.close();
    instance1.stop();
  }

  private void doTestTaskAssignmentAfterDestinationDedup(String testName, boolean compat) throws Exception {
    String testCluster = testName;
    String connectorName = "TestConnector";
    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector = new TestHookConnector("connector1", connectorName);
    coordinator.addConnector(connectorName, connector, new BroadcastStrategy(), false);
    coordinator.start();

    // Create 1st datastream
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    Datastream stream1 = DatastreamTestUtils.createAndStoreDatastreams(zkClient, testName, connectorName, "stream54321")[0];
    Assert.assertTrue(stream1.getMetadata().containsKey(DatastreamMetadataConstants.CREATION_MS));

    // Wait for first assignment is done
    Assert.assertTrue(PollUtils.poll(() -> connector.getTasks().size() == 1, 50, WAIT_TIMEOUT_MS));

    Counter numRebals = ReflectionUtils.getField(coordinator, "_numRebalances");
    Assert.assertNotNull(numRebals);

    long numAssign1 = numRebals.getCount();
    DatastreamTask task1 = connector.getTasks().get(0);

    if (compat) {
      // Remove timestamp for compat mode testing
      CachedDatastreamReader reader = ReflectionUtils.getField(coordinator, "_datastreamCache");
      Assert.assertNotNull(reader);
      reader.getDatastream(stream1.getName()).getMetadata().remove(DatastreamMetadataConstants.CREATION_MS);
    }

    // Create 2nd datastream with name alphabetically "smaller" than 1st datastream and same destination
    Datastream stream2 = stream1.clone();
    stream2.setName("stream12345");
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, stream2);

    // Wait for new assignment is done
    Assert.assertTrue(PollUtils.poll(() -> (numRebals.getCount() > numAssign1), 50, WAIT_TIMEOUT_MS));

    // Make sure connector still has only one task
    Assert.assertEquals(connector.getTasks().size(), 1);

    Assert.assertEquals(connector.getTasks().get(0), task1);
  }

  /**
   * Test the scenario where a newer datastream shares the destination as an existing
   * datastream but the name of the new datastream is alphabetically "smaller" than
   * the existing datastream. In this case, the existing datastream should be chosen
   * for task assignment after de-dup, thus without resulting in new tasks created.
   *
   * @throws Exception
   */
  @Test
  public void testTaskAssignmentAfterDestinationDedup() throws Exception {
    doTestTaskAssignmentAfterDestinationDedup("testTaskAssignmentAfterDestinationDedup", false);
  }

  /**
   * Test the same scenario as testTaskAssignmentAfterDestinationDedup but ensure
   * compatibility with existing datastreams without the timestamp in metadata.
   * In which case, the datastream without timestamp metadata is the older one.
   *
   * @throws Exception
   */
  @Test
  public void testTaskAssignmentAfterDestinationDedupCompat() throws Exception {
    doTestTaskAssignmentAfterDestinationDedup("testTaskAssignmentAfterDestinationDedupCompat", true);
  }

  private class TestSetup {
    public final EmbeddedDatastreamCluster _datastreamKafkaCluster;
    public final Coordinator _coordinator;
    public final DatastreamResources _resource;
    public final TestHookConnector _connector;

    public TestSetup(EmbeddedDatastreamCluster datastreamKafkaCluster, Coordinator coordinator,
        DatastreamResources resource, TestHookConnector connector) {
      _datastreamKafkaCluster = datastreamKafkaCluster;
      _coordinator = coordinator;
      _resource = resource;
      _connector = connector;
    }
  }

  private TestSetup createTestCoordinator() throws Exception {
    EmbeddedDatastreamCluster datastreamKafkaCluster =
        TestDatastreamServer.initializeTestDatastreamServerWithDummyConnector(null);
    datastreamKafkaCluster.startup();
    Properties properties = datastreamKafkaCluster.getDatastreamServerProperties().get(0);
    DatastreamResources resource = new DatastreamResources(datastreamKafkaCluster.getPrimaryDatastreamServer());

    Coordinator coordinator = createCoordinator(properties.getProperty(DatastreamServer.CONFIG_ZK_ADDRESS),
        properties.getProperty(DatastreamServer.CONFIG_CLUSTER_NAME));

    TestHookConnector connector = new TestHookConnector("connector1", DummyConnector.CONNECTOR_TYPE);

    coordinator.addConnector(DummyConnector.CONNECTOR_TYPE, connector, new BroadcastStrategy(), false);

    coordinator.start();

    return new TestSetup(datastreamKafkaCluster, coordinator, resource, connector);
  }

  private void validateRetention(Datastream stream, DatastreamResources resource, Duration expectedRetention) {
    Datastream queryStream = resource.get(stream.getName());
    Assert.assertNotNull(queryStream.getDestination());
    StringMap metadata = queryStream.getMetadata();
    Assert.assertNotNull(metadata.getOrDefault(DatastreamMetadataConstants.DESTINATION_CREATION_MS, null));
    Assert.assertNotNull(metadata.getOrDefault(DatastreamMetadataConstants.DESTINATION_RETENION_MS, null));
    String retentionMs = metadata.get(DatastreamMetadataConstants.DESTINATION_RETENION_MS);
    Assert.assertEquals(retentionMs, String.valueOf(expectedRetention.toMillis()));
  }

  /**
   * Test create datastream scenario with the actual DSM.
   *
   * The expected outcome includes:
   *
   * 1) a new datastream is created by DSM and can be queried by name afterwards
   * 2) the datastream has valid destination (populated by DestinationManager)
   * 3) connector is assigned the task for the datastream
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatastreamHappyPath() throws Exception {
    TestSetup setup = createTestCoordinator();

    // Check retention when it's specific in topicConfig
    Duration myRetention = Duration.ofDays(10);
    String datastreamName = "TestDatastream";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    stream.getMetadata()
        .put(DatastreamMetadataConstants.DESTINATION_RETENION_MS, String.valueOf(myRetention.toMillis()));
    CreateResponse response = setup._resource.create(stream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Make sure connector has received the assignment (timeout in 30 seconds)
    assertConnectorAssignment(setup._connector, 30000, datastreamName);
    validateRetention(stream, setup._resource, myRetention);
  }

  @Test
  public void testCreateDatastreamHappyPathDefaultRetention() throws Exception {
    TestSetup setup = createTestCoordinator();

    // Check default retention when no topicConfig is specified
    String datastreamName = "TestDatastream";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    CreateResponse response = setup._resource.create(stream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Make sure connector has received the assignment (timeout in 30 seconds)
    assertConnectorAssignment(setup._connector, 30000, datastreamName);
    validateRetention(stream, setup._resource, KafkaTransportProvider.DEFAULT_RETENTION);

    setup._datastreamKafkaCluster.shutdown();
  }

  // helper method: assert that within a timeout value, the connector are assigned the specific
  // tasks with the specified names.
  private void assertConnectorAssignment(TestHookConnector connector, long timeoutMs, String... datastreamNames)
      throws InterruptedException {

    final long interval = timeoutMs < 100 ? timeoutMs : 100;

    boolean result =
        PollUtils.poll(() -> validateAssignment(connector.getTasks(), datastreamNames), interval, timeoutMs);

    LOG.info(
        String.format("assertConnectorAssignment. Connector: %s, Connector Tasks: %s, ASSERT: %s", connector.getName(),
            connector.getTasks(), result));

    Assert.assertTrue(result);
  }

  private void waitTillAssignmentIsComplete(TestHookConnector connector1, TestHookConnector connector2,
      int totalTasks, long timeoutMs) {

    final long interval = timeoutMs < 100 ? timeoutMs : 100;

    PollUtils.poll(() -> {
      HashSet<DatastreamTask> tasks1 = new HashSet<>(connector1.getTasks());
      tasks1.addAll(connector2.getTasks());
      return tasks1.size() == totalTasks;

    }, interval, timeoutMs);

  }

  private boolean validateAssignment(List<DatastreamTask> assignment, String... datastreamNames) {

    if (assignment.size() != datastreamNames.length) {
      LOG.error("Expected size: " + datastreamNames.length + ", Actual size: " + assignment.size());
      return false;
    }

    Set<String> nameSet = new HashSet<>(Arrays.asList(datastreamNames));
    return assignment.stream().anyMatch(t -> !nameSet.contains(t.getDatastreams().get(0)) ||
        t.getEventProducer() == null);
  }

  private void deleteLiveInstanceNode(ZkClient zkClient, String cluster, Coordinator instance) {
    String path = KeyBuilder.liveInstance(cluster, instance.getInstanceName());
    zkClient.deleteRecursive(path);
  }
}
