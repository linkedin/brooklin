package com.linkedin.datastream.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamJSonUtil;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.server.assignment.SimpleStrategy;
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.server.zk.ZkClient;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestCoordinator {
  private static final Logger LOG = LoggerFactory.getLogger(TestCoordinator.class);

  private static final int waitDurationForZk = 1000;

  EmbeddedZookeeper _embeddedZookeeper;
  String _zkConnectionString;

  private Coordinator createCoordinator(String zkAddr, String cluster) {
    return createCoordinator(zkAddr, cluster, ZkClient.DEFAULT_SESSION_TIMEOUT, ZkClient.DEFAULT_CONNECTION_TIMEOUT);
  }

  private Coordinator createCoordinator(String zkAddr, String cluster, int sessionTimeout, int connectionTimeout) {
    Properties props = new Properties();
    props.put("datastream.server.coordinator.cluster", cluster);
    props.put("datastream.server.coordinator.zkAddress", zkAddr);
    props.put("datastream.server.coordinator.zkSessionTimeout", String.valueOf(sessionTimeout));
    props.put("datastream.server.coordinator.zkConnectionTime", String.valueOf(connectionTimeout));
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

  public static class TestHookConnector implements Connector {
    boolean _isStarted = false;
    String _connectorType = "TestConnector";
    List<DatastreamTask> _tasks = new ArrayList<>();

    public TestHookConnector() {

    }

    public TestHookConnector(String connectorType) {
      _connectorType = connectorType;
    }

    public boolean isStarted() {
      return _isStarted;
    }

    public boolean isStopped() {
      return !_isStarted;
    }

    public List<DatastreamTask> getTasks() {
      return _tasks;
    }

    @Override
    public void start(DatastreamEventCollector collector) {
      _isStarted = true;
    }

    @Override
    public void stop() {
      _isStarted = false;
    }

    @Override
    public synchronized void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {
      _tasks = tasks;
    }

    @Override
    public DatastreamTarget getDatastreamTarget(Datastream stream) {
      return null;
    }

    @Override
    public DatastreamValidationResult validateDatastream(Datastream stream) {
      return new DatastreamValidationResult();
    }

    @Override
    public String getConnectorType() {
      return _connectorType;
    }
  }

  @Test
  public void testConnectorStartStop() throws Exception {
    String testCluster = "test_coordinator_startstop";
    Coordinator coordinator = createCoordinator(_zkConnectionString, testCluster);

    TestHookConnector connector = new TestHookConnector();
    coordinator.addConnector(connector, new BroadcastStrategy());

    coordinator.start();
    Assert.assertTrue(connector.isStarted());

    coordinator.stop();
    Assert.assertTrue(connector.isStopped());
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
      public void start(DatastreamEventCollector collector) {
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
        return null;
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
    int retries = 5;
    while (retries >= 0
        && !zkClient.exists(KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, datastreamName1, "",
            "counter"))) {
      Thread.sleep(waitDurationForZk);
      retries--;
    }
    String counter =
        zkClient.readData(KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, datastreamName1, "",
            "counter"));
    Assert.assertEquals(counter, "1");
    //
    // add a second datastream named datastream2
    //
    String datastreamName2 = "datastream2";
    createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName2);
    retries = 5;
    while (retries >= 0
        && !zkClient.exists(KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, datastreamName2, "",
            "counter"))) {
      Thread.sleep(waitDurationForZk);
      retries--;
    }
    //
    // verify that the counter for datastream1 is "2" but the counter for datastream2 is "1"
    //
    counter =
        zkClient.readData(KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, datastreamName1, "",
            "counter"));
    Assert.assertEquals(counter, "2");
    counter =
        zkClient.readData(KeyBuilder.datastreamTaskStateKey(testCluster, testConectorType, datastreamName2, "",
            "counter"));
    Assert.assertEquals(counter, "1");

    //
    // clean up
    //
    zkClient.close();
    coordinator.stop();

  }

  // testCoordinationWithBroadcastStrategy is a smoke test, to verify that datastreams created by DSM can be
  // assigned to live instances. The datastreams created by DSM is mocked by directly creating
  // the znodes in zookeeper.
  @Test
  public void testCoordinationWithBroadcastStrategy() throws Exception {
    String testCluster = "testCoordinationWithBroadcastStrategy";
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
    // wait for assignment to be done
    //
    Thread.sleep(waitDurationForZk);

    //
    // verify the instance has 1 task assigned: datastream1
    //
    List<DatastreamTask> assigned1 = connector1.getTasks();
    assertDatastreamTaskAssignments(assigned1, "datastream1");

    //
    // create a second live instance named instance2 and join the cluster
    //
    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2 = new TestHookConnector(testConectorType);
    instance2.addConnector(connector2, new BroadcastStrategy());
    instance2.start();

    //
    // verify instance2 has 1 task assigned
    //
    List<DatastreamTask> assigned2 = connector2.getTasks();
    int retries = 10;
    while (retries >= 0 && assigned2.size() != 1) {
      Thread.sleep(waitDurationForZk);
      assigned2 = connector2.getTasks();
      retries--;
    }

    Assert.assertEquals(assigned2.size(), 1, "expect: 1, actual: " + assigned2.size());

    //
    // create a new datastream definition for the same connector type, /testAssignmentBasic/datastream/datastream2
    //
    String datastreamName2 = "datastream2";
    createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName2);

    //
    // wait for the leader to rebalance for the new live instance
    //
    Thread.sleep(waitDurationForZk);

    //
    // verify both instance1 and instance2 now have two datastreamtasks assigned
    //
    assigned1 = connector1.getTasks();
    assigned2 = connector2.getTasks();
    assertDatastreamTaskAssignments(assigned1, "datastream1", "datastream2");
    assertDatastreamTaskAssignments(assigned2, "datastream1", "datastream2");

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
    // wait for assignment to finish
    //
    Thread.sleep(waitDurationForZk);

    //
    // verify both live instances have tasks assigned for connector type 1 only
    //
    List<DatastreamTask> assigned11 = connector11.getTasks();
    List<DatastreamTask> assigned12 = connector12.getTasks();
    List<DatastreamTask> assigned21 = connector21.getTasks();
    List<DatastreamTask> assigned22 = connector22.getTasks();
    Assert.assertEquals(assigned11.size(), 1);
    Assert.assertEquals(assigned12.size(), 0);
    Assert.assertEquals(assigned21.size(), 1);
    Assert.assertEquals(assigned22.size(), 0);

    //
    // create a new datastream for connectorType2
    //
    createDatastreamForDSM(zkClient, testCluster, connectorType2, "datastream2");

    //
    // wait for assignment to finish
    //
    Thread.sleep(waitDurationForZk);

    //
    // verify both live instances have tasks assigned for both connector types
    //
    assigned11 = connector11.getTasks();
    assigned12 = connector12.getTasks();
    assigned21 = connector21.getTasks();
    assigned22 = connector22.getTasks();
    Assert.assertEquals(assigned11.size(), 1);
    Assert.assertEquals(assigned12.size(), 1);
    Assert.assertEquals(assigned21.size(), 1);
    Assert.assertEquals(assigned22.size(), 1);

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
      Runnable task =
          () -> {
            Coordinator instance =
                createCoordinator(_zkConnectionString, testCluster, waitDurationForZk * 10, waitDurationForZk * 20);
            instance.start();

            // keep the thread alive
            try {
              Thread.sleep(duration);
              instance.stop();
            } catch (InterruptedException ie) {
              ie.printStackTrace();
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
    Thread.sleep(waitDurationForZk);
    //
    // verify both datastreams are assigned to instance1
    //
    List<DatastreamTask> assignment1 = connector1.getTasks();
    assertDatastreamTaskAssignments(assignment1, "datastream0", "datastream1");
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
    assignment1 = connector1.getTasks();
    assertDatastreamTaskAssignments(assignment1, "datastream0");
    List<DatastreamTask> assignment2 = connector2.getTasks();
    assertDatastreamTaskAssignments(assignment2, "datastream1");
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
    assignment1 = connector1.getTasks();
    assertDatastreamTaskAssignments(assignment1, "datastream0");
    assignment2 = connector2.getTasks();
    assertDatastreamTaskAssignments(assignment2, "datastream1");
    List<DatastreamTask> assignment3 = connector3.getTasks();
    assertDatastreamTaskAssignments(assignment3);

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
    Coordinator instance2 =
        createCoordinator(_zkConnectionString, testCluster, waitDurationForZk * 2, waitDurationForZk * 5);
    TestHookConnector connector2 = new TestHookConnector(testConnectoryType);
    instance2.addConnector(connector2, new SimpleStrategy());
    instance2.start();

    //
    // create 4 datastreams, [datastream0, datastream1, datatream2, datastream3]
    //
    for (int i = 0; i < 4; i++) {
      createDatastreamForDSM(zkClient, testCluster, testConnectoryType, datastreamName + i);
    }
    Thread.sleep(waitDurationForZk);

    //
    // verify assignment, instance1: [datastream0, datastream2], instance2:[datastream1, datastream3]
    //
    List<DatastreamTask> assigned1 = connector1.getTasks();
    List<DatastreamTask> assigned2 = connector2.getTasks();

    assertDatastreamTaskAssignments(assigned1, "datastream0", "datastream2");
    assertDatastreamTaskAssignments(assigned2, "datastream1", "datastream3");

    //
    // take instance2 offline
    //
    instance2.stop();
    // wait lone enough for zookeeper to remove the live instance
    Thread.sleep(waitDurationForZk * 3);

    //
    // verify all 4 datastreams are assigned to instance1
    //
    assigned1 = connector1.getTasks();
    assertDatastreamTaskAssignments(assigned1, "datastream0", "datastream1", "datastream2", "datastream3");

    //
    // clean up
    //
    instance1.stop();
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
    List<DatastreamTask> assignment1 = connector1.getTasks();
    List<DatastreamTask> assignment2 = connector2.getTasks();
    assertDatastreamTaskAssignments(assignment1, "datastream1");
    assertDatastreamTaskAssignments(assignment2, "datastream2");
    //
    // create 1 new datastream "datastream0", which has the smallest lexicographical order
    //
    createDatastreamForDSM(zkClient, testCluster, testConnectoryType, "datastream0");
    Thread.sleep(waitDurationForZk);
    //
    // verify assignment instance1:[datastream0, datastream2], instance2:[datastream1]
    //
    assignment1 = connector1.getTasks();
    assignment2 = connector2.getTasks();
    assertDatastreamTaskAssignments(assignment1, "datastream0", "datastream2");
    assertDatastreamTaskAssignments(assignment2, "datastream1");

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
    String testCluster = "testSimpleAssignmentStrategyIndependent";
    String connectoryType1 = "ConnectoryType1";
    String connectoryType2 = "ConnectoryType2";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // setup a cluster with 2 live instances with simple assignment strategy,
    // each has two connectors
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1a = new TestHookConnector(connectoryType1);
    TestHookConnector connector1b = new TestHookConnector(connectoryType2);
    instance1.addConnector(connector1a, new SimpleStrategy());
    instance1.addConnector(connector1b, new BroadcastStrategy());
    instance1.start();

    Coordinator instance2 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector2a = new TestHookConnector(connectoryType1);
    TestHookConnector connector2b = new TestHookConnector(connectoryType2);
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
    Thread.sleep(waitDurationForZk);
    //
    // verify assignment: instance1.connector1: [datastream0], connector2:[datastream2, datastream4"]
    // instance2.connector1:[datastream1], connector2:[datastream3]
    //
    List<DatastreamTask> assignment = connector1a.getTasks();
    assertDatastreamTaskAssignments(assignment, "simple0", "simple2");
    assignment = connector1b.getTasks();
    assertDatastreamTaskAssignments(assignment, "broadcast0", "broadcast1", "broadcast2");
    assignment = connector2a.getTasks();
    assertDatastreamTaskAssignments(assignment, "simple1");
    assignment = connector2b.getTasks();
    assertDatastreamTaskAssignments(assignment, "broadcast0", "broadcast1", "broadcast2");

    //
    // clean up
    //
    instance1.stop();
    instance2.stop();
    zkClient.close();
  }

  //
  // verify that instance and live instance zk nodes are created and are cleaned up
  // appropriatedly
  //
  @Test
  public void testZooKeeperPathForInstances() throws Exception {
    String testCluster = "testZooKeeperPathForInstances";
    String connectoryType1 = "Oracle";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // create a live instance
    //
    Coordinator instance1 =
        createCoordinator(_zkConnectionString, testCluster, waitDurationForZk * 2, waitDurationForZk * 4);
    instance1.start();

    //
    // verify the following znode exists:
    //  /{cluster}/liveinstances/{liveInstnaceName}
    //  /{cluster}/instances/{instancesName}
    //
    Assert.assertTrue(zkClient.exists(KeyBuilder.instance(testCluster, instance1.getInstanceName())));
    Assert.assertEquals(zkClient.countChildren(KeyBuilder.liveInstances(testCluster)), 1);

    //
    // create 2 datastreams and wait for the assignment to happen.
    //
    createDatastreamForDSM(zkClient, testCluster, connectoryType1, "simple0", "simple1");
    Thread.sleep(waitDurationForZk);

    //
    // verify the instance assigment is empty, because the coordinator does not have the connector
    //
    Assert.assertEquals(zkClient.countChildren(KeyBuilder.instance(testCluster, instance1.getInstanceName())), 0);

    //
    // now stop instance1, and create instance2 with the connector. The new instance2 will become the new leader
    // and it will proactively remove instance nodes if it is not live anymore.
    //
    instance1.stop();
    Coordinator instance2 =
        createCoordinator(_zkConnectionString, testCluster, waitDurationForZk * 2, waitDurationForZk * 4);
    TestHookConnector connector2 = new TestHookConnector(connectoryType1);
    instance2.addConnector(connector2, new SimpleStrategy());
    instance2.start();
    Thread.sleep(waitDurationForZk); // wait for instance1 to actually die

    //
    // verify all znodes for instance1 has been cleaned up
    //
    Assert.assertEquals(zkClient.countChildren(KeyBuilder.liveInstances(testCluster)), 1);
    Assert.assertFalse(zkClient.exists(KeyBuilder.instance(testCluster, instance1.getInstanceName())));

    //
    // verify the instance2 node has 2 children for the assignment
    //
    List<String> assignment = zkClient.getChildren(KeyBuilder.instance(testCluster, instance2.getInstanceName()));
    Assert.assertEquals(assignment.size(), 2);
    Assert.assertTrue(assignment.contains("simple0"));
    Assert.assertTrue(assignment.contains("simple1"));

    //
    // clean up
    //
    zkClient.close();
    instance2.stop();

  }

  //
  // verify that when an instance dies, all related zookeeper data are removed, including current assignment
  // and live instance nodes
  //
  @Test
  public void testZooKeeperPathForDatastream() throws Exception {
    String testCluster = "testZooKeeperPathForDatastream";
    String connectoryType1 = "ConnectoryType1";
    String connectoryType2 = "ConnectoryType2";
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    //
    // setup a cluster with 2 live instances with simple assignment strategy,
    // each has two connectors
    //
    Coordinator instance1 = createCoordinator(_zkConnectionString, testCluster);
    TestHookConnector connector1a = new TestHookConnector(connectoryType1);
    TestHookConnector connector1b = new TestHookConnector(connectoryType2);
    instance1.addConnector(connector1a, new SimpleStrategy());
    instance1.addConnector(connector1b, new BroadcastStrategy());
    instance1.start();
    Thread.sleep(waitDurationForZk);

    //
    // verify the existence of live instance node, live instance node
    //
    String instanceNode = KeyBuilder.instance(testCluster, instance1.getInstanceName());
    Assert.assertTrue(zkClient.exists(instanceNode));
    int numLiveInstances = zkClient.countChildren(KeyBuilder.liveInstances(testCluster));
    Assert.assertEquals(numLiveInstances, 1);

    //
    // create datastreams
    //
    createDatastreamForDSM(zkClient, testCluster, connectoryType1, "simple0", "simple1");
    createDatastreamForDSM(zkClient, testCluster, connectoryType2, "broadcast0", "broadcast1");
    Thread.sleep(waitDurationForZk);
    //
    // verify datastream config/state path exists
    //
    assertDatastreamTaskZkPaths(zkClient, testCluster, connectoryType1, "simple0", "simple1");
    assertDatastreamTaskZkPaths(zkClient, testCluster, connectoryType2, "broadcast0", "broadcast1");

    //
    // take instance1 offline
    //
    instance1.stop();
    Thread.sleep(waitDurationForZk);

    //
    // verify live instance and instance node is gone
    //
    Assert.assertFalse(zkClient.exists(instanceNode));
    numLiveInstances = zkClient.countChildren(KeyBuilder.liveInstances(testCluster));
    Assert.assertEquals(numLiveInstances, 0);
    //
    // verify config and state path still exists
    //
    assertDatastreamTaskZkPaths(zkClient, testCluster, connectoryType1, "simple0", "simple1");
    assertDatastreamTaskZkPaths(zkClient, testCluster, connectoryType2, "broadcast0", "broadcast1");

    //
    // clean up
    //
    instance1.stop();
    zkClient.close();
  }

  //
  // assert that zookeeper path (config and state) for datastreamtask exists.
  //
  private void assertDatastreamTaskZkPaths(ZkClient zkClient, String cluster, String connectorType,
      String... datastreataskNames) {
    for (String datastreamTaskName : datastreataskNames) {
      String configPath = KeyBuilder.datastreamTaskConfig(cluster, connectorType, datastreamTaskName);
      Assert.assertTrue(zkClient.exists(configPath));
      String statePath = KeyBuilder.datastreamTaskState(cluster, connectorType, datastreamTaskName);
      Assert.assertTrue(zkClient.exists(statePath));
    }
  }

  //
  // helper method: verify the assigned DatastreamTask matches the datastreamNames list
  //
  private void assertDatastreamTaskAssignments(List<DatastreamTask> assignment, String... datastreamNames) {
    Assert.assertEquals(assignment.size(), datastreamNames.length);

    if (assignment.size() == 0) {
      return;
    }

    List<String> list = Arrays.asList(datastreamNames);

    assignment.forEach(ds -> Assert.assertTrue(list.contains(ds.getDatastreamName()),
        "DatastreamTask with name " + ds.getDatastreamTaskName() + " is not expected"));
  }

  private void createDatastreamForDSM(ZkClient zkClient, String cluster, String connectorType,
      String... datastreamNames) {
    for (String datastreamName : datastreamNames) {
      zkClient.ensurePath(KeyBuilder.datastreams(cluster));
      Datastream datastream = new Datastream();
      datastream.setName(datastreamName);
      datastream.setConnectorType(connectorType);
      String json = DatastreamJSonUtil.getJSonStringFromDatastream(datastream);
      zkClient.create(KeyBuilder.datastream(cluster, datastreamName), json, CreateMode.PERSISTENT);
    }
  }
}
