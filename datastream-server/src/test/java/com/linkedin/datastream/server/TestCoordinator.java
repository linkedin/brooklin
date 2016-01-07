package com.linkedin.datastream.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.testutil.DatastreamTestUtils;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.server.assignment.SimpleStrategy;
import com.linkedin.datastream.server.dms.DatastreamResources;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.restli.common.HttpStatus;
import com.linkedin.restli.server.CreateResponse;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;


public class TestCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(TestCoordinator.class);
  private static final String TRANSPORT_FCTORY_CLASS = DummyTransportProviderFactory.class.getTypeName();
  private static final int waitDurationForZk = 1000;
  private static final int waitTimeoutMS = 30000;

  EmbeddedZookeeper _embeddedZookeeper;
  String _zkConnectionString;

  private Coordinator createCoordinator(String zkAddr, String cluster) throws Exception {
    Properties props = new Properties();
    props.put(CoordinatorConfig.CONFIG_CLUSTER, cluster);
    props.put(CoordinatorConfig.CONFIG_ZK_ADDRESS, zkAddr);
    props.put(CoordinatorConfig.CONFIG_ZK_SESSION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_SESSION_TIMEOUT));
    props.put(CoordinatorConfig.CONFIG_ZK_CONNECTION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_CONNECTION_TIMEOUT));
    props.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, TRANSPORT_FCTORY_CLASS);

    return new Coordinator(props);
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
        Assert.assertNotNull(task.getEventProducer());
      }

      LOG.info("END: onAssignmentChange");
    }

    @Override
    public void initializeDatastream(Datastream stream) {
    }

    @Override
    public String toString() {
      return "Connector " + _name + ", StatusId: " + _connectorType + ", Instance: " + _instance;
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
  @Test(enabled = false)
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
      public void initializeDatastream(Datastream stream) {

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
    TestHookConnector connector1 = new TestHookConnector(testConectorType);
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
    TestHookConnector connector1 = new TestHookConnector(testConectorType);
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
    assertConnectorAssignment(connector1, waitTimeoutMS, datastreamName1);

    //
    // create a second live instance named instance2 and join the cluster
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConectorType);
    instance2.addConnector(testConectorType, connector2, new BroadcastStrategy(), false);
    instance2.start();

    //
    // verify instance2 has 1 task assigned
    //
    assertConnectorAssignment(connector2, waitTimeoutMS, datastreamName1);

    //
    // create a new datastream definition for the same connector type, /testAssignmentBasic/datastream/datastream2
    //
    String datastreamName2 = "datastream2";
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConectorType, datastreamName2);

    //
    // verify both instance1 and instance2 now have two datastreamtasks assigned
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream1", "datastream2");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream1", "datastream2");

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
  }

  // This test verify the code path that a datastream is only assignable after it has a valid
  // destination setup. That is, if a datastream is defined for a connector but DestinationManager
  // failed to populate its destination, we will not actually pass this datastream to the connector
  // using onAssignmentChange, so that the connector would not start producing events for this
  // datastream when there is no target to accept it.
  @Test
  public void testUnassignableStreams() throws Exception {

    String testCluster = "testUnassignableStreams";
    String connectorType1 = "unassignable";
    String connectorType2 = "assignable";

    //
    // create two connectors
    //
    TestHookConnector connector1 = new TestHookConnector(connectorType1);
    TestHookConnector connector2 = new TestHookConnector(connectorType2);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    DestinationManager destinationManager = mock(DestinationManager.class);
    // Hijack the destinationManager to selectively populate destinations
    ReflectionUtils.setField(instance1, "_destinationManager", destinationManager);

    instance1.addConnector(connectorType1, connector1, new BroadcastStrategy(), false);
    instance1.addConnector(connectorType2, connector2, new BroadcastStrategy(), false);
    instance1.start();

    //
    // create 2 new datastreams, 1 for each type
    //
    LOG.info("create 2 new datastreams, 1 for each type");
    Datastream[] streams1 = DatastreamTestUtils.createDatastreams(connectorType1, "datastream1");
    Datastream[] streams2 = DatastreamTestUtils.createDatastreams(connectorType2, "datastream2");

    // Stub for populateDatastreamDestination to set destination on datastream2
    LOG.info("Set destination only for datastream2, leave it unassigned for datastream1");
    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      List<Datastream> streams = (List<Datastream>) args[0];
      for (Datastream stream : streams) {
        // Populate Destination only for datastream2
        if (stream.getName().equals("datastream2")) {
          setDatastreamDestination(stream);
        }
      }
      return null;
    }).when(destinationManager).populateDatastreamDestination(any());

    ZkClient zkClient = new ZkClient(_zkConnectionString);
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, streams1);
    DatastreamTestUtils.storeDatastreams(zkClient, testCluster, streams2);

    //
    // verify only connector2 has assignment
    //
    LOG.info("verify only connector2 has assignment");
    assertConnectorAssignment(connector1, waitTimeoutMS);
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream2");

    LOG.info("enable destination for all datastreams including datastream1");
    // Stub for populateDatastreamDestination to set destination on all datastreams
    Mockito.doAnswer(invocation -> {
      Object[] args = invocation.getArguments();
      List<Datastream> streams = (List<Datastream>) args[0];
      for (Datastream stream : streams) {
        setDatastreamDestination(stream);
      }
      return null;
    }).when(destinationManager).populateDatastreamDestination(any());

    //
    // trigger the datastream change event
    //
    instance1.onDatastreamChange();

    //
    // verify both connectors have 1 assignment.
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream2");

    //
    // clean up
    //
    zkClient.close();
    instance1.stop();
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

    //
    // create two live instances, each handle two different types of connectors
    //
    TestHookConnector connector11 = new TestHookConnector(connectorType1);
    TestHookConnector connector12 = new TestHookConnector(connectorType2);

    TestHookConnector connector21 = new TestHookConnector(connectorType1);
    TestHookConnector connector22 = new TestHookConnector(connectorType2);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    instance1.addConnector(connectorType1, connector11, new BroadcastStrategy(), false);
    instance1.addConnector(connectorType2, connector12, new BroadcastStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    instance2.addConnector(connectorType1, connector21, new BroadcastStrategy(), false);
    instance2.addConnector(connectorType2, connector22, new BroadcastStrategy(), false);
    instance2.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create a new datastream for connectorType1
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType1, "datastream1");

    //
    // verify both live instances have tasks assigned for connector type 1 only
    //
    assertConnectorAssignment(connector11, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector12, waitTimeoutMS);

    assertConnectorAssignment(connector21, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector22, waitTimeoutMS);

    //
    // create a new datastream for connectorType2
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectorType2, "datastream2");

    //
    // verify both live instances have tasks assigned for both connector types
    //
    assertConnectorAssignment(connector11, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector12, waitTimeoutMS, "datastream2");

    assertConnectorAssignment(connector21, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector22, waitTimeoutMS, "datastream2");

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
    int duration = waitDurationForZk * 5;

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

    Assert.assertEquals(instances.size(), concurrencyLevel);
    zkClient.close();
  }

  // this is a potentially flaky test
  @Test
  public void testStressLargeNumberOfDatastreams() throws Exception {

    int concurrencyLevel = 10;

    String testCluster = "testStressLargeNumberOfDatastreams";
    String testConectorType = "testConnectorType";
    String datastreamName = "datastream";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create 1 live instance and start it
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector(testConectorType);
    instance1.addConnector(testConectorType, connector1, new BroadcastStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);

    TestHookConnector connector2 = new TestHookConnector(testConectorType);
    instance2.addConnector(testConectorType, connector2, new BroadcastStrategy(), false);
    instance2.start();

    String[] datastreamNames = new String[concurrencyLevel];

    //
    // create large number of datastreams
    //
    for (int i = 0; i < concurrencyLevel; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConectorType, datastreamName + i);
      datastreamNames[i] = datastreamName + i;
    }

    assertConnectorAssignment(connector1, waitTimeoutMS, datastreamNames);
    assertConnectorAssignment(connector2, waitTimeoutMS, datastreamNames);

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

    //
    // create 1 instance
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector(testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new SimpleStrategy(), false);
    instance1.start();

    //
    // create 2 datastreams, [datastream0, datastream1]
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream0");
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream1");

    //
    // verify both datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream0", "datastream1");

    //
    // add a new live instance instance2
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new SimpleStrategy(), false);
    instance2.start();

    //
    // verify new assignment. instance1 : [datastream0], instance2: [datastream1]
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream0");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream1");
    //
    // add instance3
    //
    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector(testConnectoryType);
    instance3.addConnector(testConnectoryType, connector3, new SimpleStrategy(), false);
    instance3.start();

    //
    // verify assignment didn't change
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream0");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector3, waitTimeoutMS);
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

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector(testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new SimpleStrategy(), false);
    instance1.start();

    // make sure the instance2 can be taken offline cleanly with session expiration
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new SimpleStrategy(), false);
    instance2.start();

    //
    // create 4 datastreams, [datastream0, datastream1, datatream2, datastream3]
    //
    for (int i = 0; i < 4; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, waitDurationForZk * 2, "datastream0", "datastream2");
    assertConnectorAssignment(connector2, waitDurationForZk * 2, "datastream1", "datastream3");

    List<DatastreamTask> tasks1 = new ArrayList<>(connector1.getTasks());
    tasks1.addAll(connector2.getTasks());
    Collections.sort(tasks1, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    //
    // take instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    //
    // verify all 4 datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream0", "datastream1", "datastream2", "datastream3");

    // Make sure strategy reused all tasks as opposed to creating new ones
    List<DatastreamTask> tasks2 = new ArrayList<>(connector1.getTasks());
    Collections.sort(tasks2, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));
    Assert.assertEquals(tasks1, tasks2);

    // Verify dead instance assignments have been removed
    // Assert.assertTrue(!zkClient.exists(KeyBuilder.instanceAssignments(testCluster, instance2.getInstanceName())));

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

    //
    // setup a cluster with 3 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector(testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new SimpleStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new SimpleStrategy(), false);
    instance2.start();

    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector(testConnectoryType);
    instance3.addConnector(testConnectoryType, connector3, new SimpleStrategy(), false);
    instance3.start();

    //
    // create 6 datastreams, [datastream0, ..., datastream5]
    //
    for (int i = 0; i < 6; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, waitDurationForZk * 2, "datastream0", "datastream3");
    assertConnectorAssignment(connector2, waitDurationForZk * 2, "datastream1", "datastream4");
    assertConnectorAssignment(connector3, waitDurationForZk * 2, "datastream2", "datastream5");

    List<DatastreamTask> tasks1 = new ArrayList<>(connector1.getTasks());
    tasks1.addAll(connector2.getTasks());
    tasks1.addAll(connector3.getTasks());
    Collections.sort(tasks1, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));

    //
    // take current leader instance1 offline
    //
    instance1.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance1);

    //
    // verify all 6 datastreams are assigned to instance2 and instance3
    //
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream0", "datastream2", "datastream4");
    assertConnectorAssignment(connector3, waitTimeoutMS, "datastream1", "datastream3", "datastream5");

    //
    // take current leader instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    //
    // verify all tasks assigned to instance3
    assertConnectorAssignment(connector3, waitTimeoutMS, "datastream0", "datastream2", "datastream4", "datastream1",
        "datastream3", "datastream5");

    // Make sure strategy reused all tasks as opposed to creating new ones
    List<DatastreamTask> tasks2 = new ArrayList<>(connector3.getTasks());
    Collections.sort(tasks2, (o1, o2) -> o1.getDatastreamTaskName().compareTo(o2.getDatastreamTaskName()));
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

    //
    // create a list of instances
    //
    int count = 4;
    Coordinator[] coordinators = new Coordinator[count];
    TestHookConnector[] connectors = new TestHookConnector[count];
    for (int i = 0; i < count; i++) {
      coordinators[i] = createCoordinator(_zkConnectionString, testCluster);
      connectors[i] = new TestHookConnector(testConnectoryType);
      coordinators[i].addConnector(testConnectoryType, connectors[i], new SimpleStrategy(), false);
      coordinators[i].start();
    }

    //
    // create 1 datastream per instance
    //
    for (int i = 0; i < count; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    //
    // wait until the last instance was assigned the last datastream, by now all datastream should be assigned
    //
    assertConnectorAssignment(connectors[count - 1], waitTimeoutMS, "datastream" + (count - 1));

    //
    // kill all instances except the current leader
    //
    for (int i = 1; i < count; i++) {
      coordinators[i].stop();
      deleteLiveInstanceNode(zkClient, testCluster, coordinators[i]);
    }

    //
    // validate all datastream tasks are assigned to the leader now
    //
    String[] assignment = new String[count];
    for (int i = 0; i < count; i++) {
      assignment[i] = datastreamName + i;
    }
    assertConnectorAssignment(connectors[0], waitTimeoutMS, assignment);

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

    //
    // setup a cluster with 2 live instances with simple assignment strategy
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1 = new TestHookConnector(testConnectoryType);
    instance1.addConnector(testConnectoryType, connector1, new SimpleStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(testConnectoryType, connector2, new SimpleStrategy(), false);
    instance2.start();

    //
    // create 2 datastreams [datastream1, datastream2]
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream1");
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream2");
    //
    // verify assignment instance1: [datastream1], instance2:[datastream2]
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream2");
    //
    // create 1 new datastream "datastream0", which has the smallest lexicographical order
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, testConnectoryType, "datastream0");
    //
    // verify assignment instance1:[datastream0, datastream2], instance2:[datastream1]
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream0", "datastream2");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream1");

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

    //
    // setup a cluster with 2 live instances with simple assignment strategy,
    // each has two connectors
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1a = new TestHookConnector("connector1a", connectoryType1);
    TestHookConnector connector1b = new TestHookConnector("connector1b", connectoryType2);
    instance1.addConnector(connectoryType1, connector1a, new SimpleStrategy(), false);
    instance1.addConnector(connectoryType2, connector1b, new BroadcastStrategy(), false);
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2a = new TestHookConnector("connector2a", connectoryType1);
    TestHookConnector connector2b = new TestHookConnector("connector2b", connectoryType2);
    instance2.addConnector(connectoryType1, connector2a, new SimpleStrategy(), false);
    instance2.addConnector(connectoryType2, connector2b, new BroadcastStrategy(), false);
    instance2.start();

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
    //
    // verify assignment: instance1.connector1: [datastream0], connector2:[datastream2, datastream4"]
    // instance2.connector1:[datastream1], connector2:[datastream3]
    //
    assertConnectorAssignment(connector1a, waitTimeoutMS, "simple0", "simple2");
    assertConnectorAssignment(connector1b, waitTimeoutMS, "broadcast0", "broadcast1", "broadcast2");
    assertConnectorAssignment(connector2a, waitTimeoutMS, "simple1");
    assertConnectorAssignment(connector2b, waitTimeoutMS, "broadcast0", "broadcast1", "broadcast2");

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
    public void initializeDatastream(Datastream stream) throws DatastreamValidationException {

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
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 0, 500, waitTimeoutMS);

    //
    // create a new datastream, which will trigger the error path
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType1, "datastream0");

    //
    // validate the error nodes now has 1 child
    //
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 1, 500, waitTimeoutMS);

    //
    // create another datastream, and validate the error nodes have 2 children
    //
    DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType1, "datastream1");
    PollUtils.poll(() -> zkClient.countChildren(errorPath) == 2, 500, waitTimeoutMS);

    //
    // create 10 more datastream, and validate the error children is caped at 10
    //
    for (int i = 2; i < 12; i++) {
      DatastreamTestUtils.createAndStoreDatastreams(zkClient, testCluster, connectoryType1, "datastream" + i);
    }
    PollUtils.poll(() -> connector1.getAssignmentCount() == 12, 200, waitDurationForZk * 5);
    int childrenCount = zkClient.countChildren(errorPath);
    Assert.assertTrue(childrenCount <= 10);

    //
    // clean up
    //
    zkClient.close();
    instance1.stop();

  }

  /**
   * Test create datastream sceanrio with the actual DSM.
   *
   * The expected outcome include:
   *
   * 1) a new datastream is created by DSM and can be queried by name afterwards
   * 2) the datastream has valid destination (populated by DestinationManager)
   * 3) connector is assigned the task for the datastream
   *
   * @throws Exception
   */
  @Test
  public void testCreateDatastreamHappyPath() throws Exception {
    EmbeddedDatastreamCluster datastreamKafkaCluster = TestDatastreamServer.initializeTestDatastreamServer(null);
    datastreamKafkaCluster.startup();
    Properties properties = datastreamKafkaCluster.getDatastreamServerProperties();
    DatastreamResources resource = new DatastreamResources(datastreamKafkaCluster.getPrimaryDatastreamServer());

    Coordinator coordinator =
        createCoordinator(properties.getProperty(DatastreamServer.CONFIG_ZK_ADDRESS),
            properties.getProperty(DatastreamServer.CONFIG_CLUSTER_NAME));

    TestHookConnector connector = new TestHookConnector(DummyConnector.CONNECTOR_TYPE);

    coordinator.addConnector(DummyConnector.CONNECTOR_TYPE, connector, new BroadcastStrategy(), false);
    coordinator.start();

    String datastreamName = "TestDatastream";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    CreateResponse response = resource.create(stream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Make sure connector has received the assignment (timeout in 30 seconds)
    assertConnectorAssignment(connector, 30000, datastreamName);

    Datastream queryStream = resource.get(stream.getName());
    Assert.assertNotNull(queryStream.getDestination());
    datastreamKafkaCluster.shutdown();
  }

  @Test
  public void testEndToEndHappyPath() throws Exception {
    EmbeddedDatastreamCluster datastreamKafkaCluster = TestDatastreamServer.initializeTestDatastreamServer(null);
    datastreamKafkaCluster.startup();
    Properties properties = datastreamKafkaCluster.getDatastreamServerProperties();
    DatastreamResources resource = new DatastreamResources(datastreamKafkaCluster.getPrimaryDatastreamServer());

    Coordinator coordinator =
        createCoordinator(properties.getProperty(DatastreamServer.CONFIG_ZK_ADDRESS),
            properties.getProperty(DatastreamServer.CONFIG_CLUSTER_NAME));

    TestHookConnector connector = new TestHookConnector(DummyConnector.CONNECTOR_TYPE);

    coordinator.addConnector(DummyConnector.CONNECTOR_TYPE, connector, new BroadcastStrategy(), false);
    coordinator.start();

    String datastreamName = "TestDatastream";
    Datastream stream = DatastreamTestUtils.createDatastreams(DummyConnector.CONNECTOR_TYPE, datastreamName)[0];
    stream.getSource().setConnectionString(DummyConnector.VALID_DUMMY_SOURCE);
    CreateResponse response = resource.create(stream);
    Assert.assertNull(response.getError());
    Assert.assertEquals(response.getStatus(), HttpStatus.S_201_CREATED);

    // Make sure connector has received the assignment (timeout in 30 seconds)
    assertConnectorAssignment(connector, 30000, datastreamName);

    Datastream queryStream = resource.get(stream.getName());
    Assert.assertNotNull(queryStream.getDestination());
    datastreamKafkaCluster.shutdown();
  }

  // helper method: assert that within a timeout value, the connector are assigned the specific
  // tasks with the specified names.
  private void assertConnectorAssignment(TestHookConnector connector, int timeoutMs, String... datastreamNames)
      throws InterruptedException {

    final int interval = timeoutMs < 100 ? timeoutMs : 100;

    boolean result =
        PollUtils.poll(() -> validateAssignment(connector.getTasks(), datastreamNames), interval, timeoutMs);

    LOG.info("assertConnectorAssignment. Connector: " + connector.getName() + ", ASSERT: " + result);

    Assert.assertTrue(result);
  }

  private boolean validateAssignment(List<DatastreamTask> assignment, String... datastreamNames) {

    if (assignment.size() != datastreamNames.length) {
      LOG.error("Expected size: " + datastreamNames.length + ", Actual size: " + assignment.size());
      return false;
    }

    boolean result = true;
    for (DatastreamTask task : assignment) {
      Assert.assertNotNull(task.getEventProducer());
    }

    return result;
  }

  private void deleteLiveInstanceNode(ZkClient zkClient, String cluster, Coordinator instance) {
    String path = KeyBuilder.liveInstance(cluster, instance.getInstanceName());
    zkClient.deleteRecursive(path);
  }
}
