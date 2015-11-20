package com.linkedin.datastream.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.IntStream;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamTarget;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.server.assignment.SimpleStrategy;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.server.zk.ZkClient;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(TestCoordinator.class);
  private static final String COLLECTOR_CLASS = "com.linkedin.datastream.server.DummyDatastreamEventCollector";
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
    props.put(DatastreamServer.CONFIG_EVENT_COLLECTOR_CLASS_NAME, COLLECTOR_CLASS);

    return new Coordinator(new VerifiableProperties(props));
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
    DatastreamEventCollectorFactory _factory;
    String _instance = "";
    String _name;
    boolean _withTarget = true;

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

    public void useTarget(boolean withTarget) {
      _withTarget = withTarget;
    }

    @Override
    public void start(DatastreamEventCollectorFactory factory) {
      _isStarted = true;
      _factory = factory;
      LOG.info("Connector " + _name + " started");
    }

    @Override
    public void stop() {
      _isStarted = false;
    }

    @Override
    public void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {
      _instance = context.getInstanceName();

      LOG.info("START: onAssignmentChange. Name: " + _name + ", Instance: " + context.getInstanceName()
          + ", ConnectorType: " + _connectorType + ",  Number of assignments: " + tasks.size() + ", tasks: " + tasks);

      _tasks = tasks;
      for (DatastreamTask task : tasks) {
        try {
          Assert.assertNotNull(_factory.create(task.getDatastream()));
        } catch (Exception ex) {
          Assert.fail();
        }
      }

      LOG.info("END: onAssignmentChange");
    }

    @Override
    public DatastreamTarget getDatastreamTarget(Datastream stream) {
      return _withTarget ? new DatastreamTarget("dummyTopic", 1, "localhost:11111") : null;
    }

    @Override
    public DatastreamValidationResult validateDatastream(Datastream stream) {
      return new DatastreamValidationResult();
    }

    @Override
    public String getConnectorType() {
      return _connectorType;
    }

    @Override
    public String toString() {
      return "Connector " + _name + ", Type: " + _connectorType + ", Instance: " + _instance;
    }
  }

  /**
   * testConnectorStateSetAndGet makes sure that the connector can read and write state that
   * is specific to each DatastreamTask.
   *
   * @throws Exception
   */
  @Test
  public void testConnectorStateSetAndGet() throws Exception {
    String testCluster = "testConnectorStateSetAndGet";
    String testConectorType = "testConnectorType";

    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);

    //
    // create a Connector instance, its sole purpose is to record the number of times
    // the onAssignmentChange() is called, and it will persist this value for each
    // task
    //
    Connector testConnector = new Connector() {
      @Override
      public void start(DatastreamEventCollectorFactory factory) {
      }

      @Override
      public void stop() {
      }

      @Override
      public String getConnectorType() {
        return testConectorType;
      }

      @Override
      public synchronized void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {
        // for each instance of assigned DatastreamTask, we keep a state with the key
        // "counter". Every time onAssignmentChange() is called, we increment this counter
        // by one for each assigned task.
        tasks.forEach(task -> {
          String counter = context.getState(task, "counter");
          if (counter == null) {
            context.saveState(task, "counter", "1");
          } else {
            int c = Integer.parseInt(counter);
            context.saveState(task, "counter", Integer.toString(c + 1));
          }

        });
      }

      @Override
      public DatastreamTarget getDatastreamTarget(Datastream stream) {
        return new DatastreamTarget("dummyTopic", 1, "localhost:11111");
      }

      @Override
      public DatastreamValidationResult validateDatastream(Datastream stream) {
        return null;
      }
    };
    coordinator.addConnector(testConnector, new BroadcastStrategy());
    coordinator.start();
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    //
    // create a new datastream so that the onAssignmentChange() can be called
    //
    String datastreamName1 = "datastream1";
    createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName1);
    //
    // verify that the counter value for the connector is 1 because the onAssignmentChange
    // should be called once
    //
    String datastream1CounterPath = KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, datastreamName1,
            "", "counter");
    Assert.assertTrue(PollUtils.poll((path) -> zkClient.exists(path), 500, 30000, datastream1CounterPath));
    Assert.assertEquals(zkClient.readData(datastream1CounterPath), "1");
    //
    // add a second datastream named datastream2
    //
    String datastreamName2 = "datastream2";
    createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName2);
    String datastream2CounterPath = KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, datastreamName2,
            "", "counter");
    Assert.assertTrue(PollUtils.poll((path) -> zkClient.exists(path), 500, 30000, datastream2CounterPath));
    //
    // verify that the counter for datastream1 is "2" but the counter for datastream2 is "1"
    //
    Assert.assertEquals(zkClient.readData(datastream1CounterPath), "2");
    Assert.assertEquals(zkClient.readData(datastream2CounterPath), "1");

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
    instance1.addConnector(connector1, new BroadcastStrategy());
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
    instance1.addConnector(connector1, new BroadcastStrategy());
    instance1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create datastream definitions under /testAssignmentBasic/datastream/datastream1
    //
    createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName1);

    //
    // verify the instance has 1 task assigned: datastream1
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, datastreamName1);

    //
    // create a second live instance named instance2 and join the cluster
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConectorType);
    instance2.addConnector(connector2, new BroadcastStrategy());
    instance2.start();
    Thread.sleep(waitDurationForZk);

    //
    // verify instance2 has 1 task assigned
    //
    assertConnectorAssignment(connector2, waitTimeoutMS, datastreamName1);

    //
    // create a new datastream definition for the same connector type, /testAssignmentBasic/datastream/datastream2
    //
    String datastreamName2 = "datastream2";
    createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName2);
    Thread.sleep(waitDurationForZk);

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
  // target setup. That is, if a datastream is defined for a connector that returns null for
  // getDatastreamTarget, we will not actually pass this datastream to the connector using
  // onAssignmentChange, so that the connector would not start producing events for this
  // datastream when there is no target to accept it.
  @Test
  public void testUnassignableStreams() throws Exception {

    String testCluster = "testUnassignableStreams";
    String connectorType1 = "unassignable";
    String connectorType2 = "assignable";

    //
    // create connector that returns null target
    //
    TestHookConnector connector1 = new TestHookConnector(connectorType1);
    connector1.useTarget(false);

    //
    // create a real assignable connector
    //
    TestHookConnector connector2 = new TestHookConnector(connectorType2);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);

    instance1.addConnector(connector1, new BroadcastStrategy());
    instance1.addConnector(connector2, new BroadcastStrategy());
    instance1.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create 2 new datastreams, 1 for each type
    //
    LOG.info("create 2 new datastreams, 1 for each type");
    createDatastreamForDSM(zkClient, testCluster, connectorType1, "datastream1");
    createDatastreamForDSM(zkClient, testCluster, connectorType2, "datastream2");
    Thread.sleep(waitDurationForZk);

    //
    // verify only connector2 has assignment
    //
    LOG.info("verify only connector2 has assignment");
    assertConnectorAssignment(connector1, waitTimeoutMS);
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream2");

    //
    // now enable the connector1 with target
    //
    LOG.info("enable target for connector1");
    connector1.useTarget(true);
    //
    // trigger the datastream change event
    //
    instance1.onDatastreamChange();
    //
    // verify both connectors have 1 assignment. This is because the connector1 would trigger a
    // retry loop for handle new datastream process
    //
    Thread.sleep(1000);
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream2");

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
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
    instance1.addConnector(connector11, new BroadcastStrategy());
    instance1.addConnector(connector12, new BroadcastStrategy());
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    instance2.addConnector(connector21, new BroadcastStrategy());
    instance2.addConnector(connector22, new BroadcastStrategy());
    instance2.start();

    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create a new datastream for connectorType1
    //
    createDatastreamForDSM(zkClient, testCluster, connectorType1, "datastream1");

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
    createDatastreamForDSM(zkClient, testCluster, connectorType2, "datastream2");

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
    instance1.addConnector(connector1, new BroadcastStrategy());
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);

    TestHookConnector connector2 = new TestHookConnector(testConectorType);
    instance2.addConnector(connector2, new BroadcastStrategy());
    instance2.start();

    Thread.sleep(waitDurationForZk);

    //
    // create large number of datastreams
    //
    for (int i = 0; i < concurrencyLevel; i++) {
      createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName + i);
    }

    //
    // wait for the assignment to finish
    //
    Thread.sleep(3000);

    //
    // verify all datastreams are assigned to the instance1
    //
    List<DatastreamTask> assigned1 = connector1.getTasks();
    List<DatastreamTask> assigned2 = connector2.getTasks();

    //
    // retries to reduce the flakiness
    //
    int retries = 10;
    while (assigned1.size() < concurrencyLevel && retries > 0) {
      retries--;
      Thread.sleep(waitDurationForZk);
      assigned1 = connector1.getTasks();
    }
    Assert.assertEquals(assigned1.size(), concurrencyLevel);
    Assert.assertEquals(assigned2.size(), concurrencyLevel);

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
    instance1.addConnector(connector1, new SimpleStrategy());
    instance1.start();
    Thread.sleep(waitDurationForZk);

    //
    // create 2 datastreams, [datastream0, datastream1]
    //
    createDatastreamForDSM(zkClient, testCluster, testConnectoryType, "datastream0");
    createDatastreamForDSM(zkClient, testCluster, testConnectoryType, "datastream1");

    //
    // verify both datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream0", "datastream1");

    //
    // add a new live instance instance2
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(connector2, new SimpleStrategy());
    instance2.start();
    Thread.sleep(waitDurationForZk);

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
    instance3.addConnector(connector3, new SimpleStrategy());
    instance3.start();
    Thread.sleep(waitDurationForZk);
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
    instance1.addConnector(connector1, new SimpleStrategy());
    instance1.start();

    // make sure the instance2 can be taken offline cleanly with session expiration
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(connector2, new SimpleStrategy());
    instance2.start();

    //
    // create 4 datastreams, [datastream0, datastream1, datatream2, datastream3]
    //
    for (int i = 0; i < 4; i++) {
      createDatastreamForDSM(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    //
    // wait until assignment is done. We cannot rely on assertConnectorAssignment because
    // the first instance will initially being assigned all datastreams
    //
    Thread.sleep(waitDurationForZk * 2);

    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, waitDurationForZk * 2, "datastream0", "datastream2");
    assertConnectorAssignment(connector2, waitDurationForZk * 2, "datastream1", "datastream3");

    //
    // take instance2 offline
    //
    instance2.stop();
    deleteLiveInstanceNode(zkClient, testCluster, instance2);

    //
    // verify all 4 datastreams are assigned to instance1
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream0", "datastream1", "datastream2", "datastream3");

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
    instance1.addConnector(connector1, new SimpleStrategy());
    instance1.start();
    Thread.sleep(waitDurationForZk);

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(connector2, new SimpleStrategy());
    instance2.start();
    Thread.sleep(waitDurationForZk);

    Coordinator instance3 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector3 = new TestHookConnector(testConnectoryType);
    instance3.addConnector(connector3, new SimpleStrategy());
    instance3.start();

    //
    // create 6 datastreams, [datastream0, ..., datastream5]
    //
    for (int i = 0; i < 6; i++) {
      createDatastreamForDSM(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }

    //
    // wait until assignment is done. We cannot rely on assertConnectorAssignment because
    // the first instance will initially being assigned all datastreams
    //
    Thread.sleep(waitDurationForZk * 2);

    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    assertConnectorAssignment(connector1, waitDurationForZk * 2, "datastream0", "datastream3");
    assertConnectorAssignment(connector2, waitDurationForZk * 2, "datastream1", "datastream4");
    assertConnectorAssignment(connector3, waitDurationForZk * 2, "datastream2", "datastream5");

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
      coordinators[i].addConnector(connectors[i], new SimpleStrategy());
      coordinators[i].start();
      Thread.sleep(100);
    }

    //
    // create 1 datastream per instance
    //
    for (int i = 0; i < count; i++) {
      createDatastreamForDSM(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }
    Thread.sleep(waitDurationForZk);

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
    Thread.sleep(waitDurationForZk);

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
    instance1.addConnector(connector1, new SimpleStrategy());
    instance1.start();
    Thread.sleep(waitDurationForZk);

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(connector2, new SimpleStrategy());
    instance2.start();

    //
    // create 2 datastreams [datastream1, datastream2]
    //
    createDatastreamForDSM(zkClient, testCluster, testConnectoryType, "datastream1");
    createDatastreamForDSM(zkClient, testCluster, testConnectoryType, "datastream2");
    Thread.sleep(waitDurationForZk);
    //
    // verify assignment instance1: [datastream1], instance2:[datastream2]
    //
    assertConnectorAssignment(connector1, waitTimeoutMS, "datastream1");
    assertConnectorAssignment(connector2, waitTimeoutMS, "datastream2");
    //
    // create 1 new datastream "datastream0", which has the smallest lexicographical order
    //
    createDatastreamForDSM(zkClient, testCluster, testConnectoryType, "datastream0");
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
    instance1.addConnector(connector1a, new SimpleStrategy());
    instance1.addConnector(connector1b, new BroadcastStrategy());
    instance1.start();

    Thread.sleep(waitDurationForZk);

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2a = new TestHookConnector("connector2a", connectoryType1);
    TestHookConnector connector2b = new TestHookConnector("connector2b", connectoryType2);
    instance2.addConnector(connector2a, new SimpleStrategy());
    instance2.addConnector(connector2b, new BroadcastStrategy());
    instance2.start();

    Thread.sleep(waitDurationForZk);

    //
    // create 3 datastreams ["simple0", "simple1", "simple2"] for ConnectoryType1
    //
    createDatastreamForDSM(zkClient, testCluster, connectoryType1, "simple0", "simple1", "simple2");
    //
    // create 3 datastreams [datastream2, datastream3, datastream4] for ConnectorType2
    //
    createDatastreamForDSM(zkClient, testCluster, connectoryType2, "broadcast0", "broadcast1", "broadcast2");
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

  @Test
  public void testCoordinatorErrorHandling() throws Exception {
    String testCluster = "testCoordinatorErrorHandling";
    String connectoryType1 = "ConnectoryType1";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    Connector connector1 = new Connector() {
      @Override
      public void start(DatastreamEventCollectorFactory collectorFactory) {

      }

      @Override
      public void stop() {

      }

      @Override
      public String getConnectorType() {
        return connectoryType1;
      }

      @Override
      public void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {
        // throw a fake exception to trigger the error handling
        throw new RuntimeException();
      }

      @Override
      public DatastreamTarget getDatastreamTarget(Datastream stream) {
        return new DatastreamTarget("dummyTopic", 1, "localhost:11111");
      }

      @Override
      public DatastreamValidationResult validateDatastream(Datastream stream) {
        return new DatastreamValidationResult();
      }
    };
    instance1.addConnector(connector1, new BroadcastStrategy());
    instance1.start();
    Thread.sleep(waitDurationForZk);

    //
    // validate the error nodes has 0 child because onAssignmentChange is not triggered yet
    //
    String errorPath = KeyBuilder.instanceErrors(testCluster, instance1.getInstanceName());
    int childrenCount = zkClient.countChildren(errorPath);
    Assert.assertEquals(childrenCount, 0);

    //
    // create a new datastream, which will trigger the error path
    //
    createDatastreamForDSM(zkClient, testCluster, connectoryType1, "datastream0");
    Thread.sleep(waitDurationForZk * 2);

    //
    // validate the error nodes now has 1 child
    //
    childrenCount = zkClient.countChildren(errorPath);
    Assert.assertEquals(childrenCount, 1);

    //
    // create another datastream, and validate the error nodes have 2 children
    //
    createDatastreamForDSM(zkClient, testCluster, connectoryType1, "datastream1");
    Thread.sleep(waitDurationForZk * 2);
    childrenCount = zkClient.countChildren(errorPath);
    Assert.assertEquals(childrenCount, 2);

    //
    // create 10 more datastream, and validate the error children is caped at 10
    //
    for (int i = 2; i < 12; i++) {
      createDatastreamForDSM(zkClient, testCluster, connectoryType1, "datastream" + i);
    }
    Thread.sleep(waitDurationForZk * 5);
    childrenCount = zkClient.countChildren(errorPath);
    Assert.assertTrue(childrenCount <= 10);

    //
    // clean up
    //
    zkClient.close();
    instance1.stop();

  }

  // helper method: assert that within a timeout value, the connector are assigned the specific
  // tasks with the specified names.
  private void assertConnectorAssignment(TestHookConnector connector, int timeout, String... datastreamNames)
      throws InterruptedException {

    int totalWait = 0;
    final int interval = 500;

    List<DatastreamTask> assignment = connector.getTasks();

    boolean result = validateAssignment(assignment, datastreamNames);

    while (result == false && totalWait < timeout) {
      Thread.sleep(interval);
      totalWait += interval;
      assignment = connector.getTasks();
      result = validateAssignment(assignment, datastreamNames);
    }

    LOG.info("assertConnectorAssignment. Connector: " + connector.getName() + ", Type: " + connector.getConnectorType()
        + ", ASSERT: " + result);

    Assert.assertTrue(result);
  }

  private boolean validateAssignment(List<DatastreamTask> assignment, String... datastreamNames) {

    if (assignment.size() != datastreamNames.length) {
      LOG.error("Expected size: " + datastreamNames.length + ", Actual size: " + assignment.size());
      return false;
    }

    List<String> list = Arrays.asList(datastreamNames);

    boolean result = true;
    for (DatastreamTask task : assignment) {
      if (!list.contains(task.getDatastreamName())) {
        result = false;
        LOG.error("Missing " + task.getDatastreamName() + ", list: " + list);
        break;
      }
    }

    return result;
  }

  private void createDatastreamForDSM(ZkClient zkClient, String cluster, String connectorType,
      String... datastreamNames) {
    for (String datastreamName : datastreamNames) {
      zkClient.ensurePath(KeyBuilder.datastreams(cluster));

      Datastream datastream = new Datastream();
      datastream.setName(datastreamName);
      datastream.setConnectorType(connectorType);
      datastream.setSource(new DatastreamSource());
      datastream.getSource().setConnectionString("sampleSource");

      ZookeeperBackedDatastreamStore dsStore = new ZookeeperBackedDatastreamStore(zkClient, cluster);
      dsStore.createDatastream(datastream.getName(), datastream);
    }
  }

  private void deleteLiveInstanceNode(ZkClient zkClient, String cluster, Coordinator instance) {
    String path = KeyBuilder.liveInstance(cluster, instance.getInstanceName());
    zkClient.deleteRecursive(path);
  }
}
