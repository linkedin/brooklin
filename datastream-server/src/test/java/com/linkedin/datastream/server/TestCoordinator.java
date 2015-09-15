package com.linkedin.datastream.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamJSonUtil;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
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
    private static final Logger LOG = LoggerFactory.getLogger(TestCoordinator.class.getName());

    private static final int waitDurationForZk = 1000;

    EmbeddedZookeeper _embeddedZookeeper;
    String _zkConnectionString;

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
        public void start() {
            _isStarted = true;
        }

        @Override
        public void stop() {
            _isStarted = false;
        }

        @Override
        public void onAssignmentChange(DatastreamContext context, List<DatastreamTask> tasks) {
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
        Coordinator coordinator = new Coordinator(_zkConnectionString, testCluster);

        TestHookConnector connector = new TestHookConnector();
        coordinator.addConnector(connector, new BroadcastStrategy());

        coordinator.start();
        Assert.assertTrue(connector.isStarted());

        coordinator.stop();
        Assert.assertTrue(connector.isStopped());
    }

    // testCoordinationSmoke is a smoke test, to verify that datastreams created by DSM can be
    // assigned to live instances. The datastreams created by DSM is mocked by directly creating
    // the znodes in zookeeper.
    @Test
    public void testCoordinationSmoke() throws Exception {

        String testCluster = "testAssignmentBasic";
        String testConectorType = "testConnectorType";
        String datastreamName1 = "datastream1";

        Coordinator instance1 = new Coordinator(_zkConnectionString, testCluster);
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
        // verify the instance has 1 task assigned
        //
        List<DatastreamTask> assigned1 = connector1.getTasks();
        Assert.assertEquals(assigned1.size(), 1);

        //
        // create a second live instance named instance2 and join the cluster
        //
        Coordinator instance2 = new Coordinator(_zkConnectionString, testCluster);
        TestHookConnector connector2 = new TestHookConnector(testConectorType);
        instance2.addConnector(connector2, new BroadcastStrategy());
        instance2.start();

        //
        // wait for the instance2 to be online, and be assigned with the task as well
        //
        Thread.sleep(waitDurationForZk);

        //
        // verify instance2 has 1 task assigned
        //
        List<DatastreamTask> assigned2 = connector2.getTasks();
        Assert.assertEquals(assigned2.size(), 1);

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
        Assert.assertEquals(assigned1.size(), 2);
        Assert.assertEquals(assigned2.size(), 2);

        instance1.stop();
        zkClient.close();
    }

    @Test
    public void testCoordinationMultipleConnectorTypes() throws Exception {
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

        Coordinator instance1 = new Coordinator(_zkConnectionString, testCluster);
        instance1.addConnector(connector11, new BroadcastStrategy());
        instance1.addConnector(connector12, new BroadcastStrategy());
        instance1.start();

        Coordinator instance2 = new Coordinator(_zkConnectionString, testCluster);
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
    @Test
    public void testStressLargeNumberOfLiveInstances() throws Exception {
        int concurrencyLevel = 100;
        String testCluster = "testStressUniqueInstanceNames";
        ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newCachedThreadPool();
        ZkClient zkClient = new ZkClient(_zkConnectionString);

        // the duration of each live instance thread, make sure it is long enough to verify the result
        int duration = waitDurationForZk * 5;

        for(int i = 0; i < concurrencyLevel; i++) {
            Runnable task = () -> {
                Coordinator instance = new Coordinator(_zkConnectionString, testCluster);
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
        Thread.sleep(waitDurationForZk * 2);

        //
        // verify all instances are alive
        //
        List<String> instances = zkClient.getChildren(KeyBuilder.liveInstances(testCluster));

        Assert.assertEquals(instances.size(), concurrencyLevel);
        zkClient.close();
    }

    @Test
    public void testStressLargeNumberOfDatastreams() throws Exception {
        //
        // note: on standard issue Macbook Pro, the max capacity is creating 80
        // datastreams per second, stable capacity is 70 per second
        //
        // this is a potentially flaky test
        int concurrencyLevel = 50;

        String testCluster = "testStressLargeNumberOfDatastreams";
        String testConectorType = "testConnectorType";
        String datastreamName = "datastream";
        ZkClient zkClient = new ZkClient(_zkConnectionString);

        //
        // create 1 live instance and start it
        //
        Coordinator instance1 = new Coordinator(_zkConnectionString, testCluster);
        TestHookConnector connector1 = new TestHookConnector(testConectorType);
        instance1.addConnector(connector1, new BroadcastStrategy());
        instance1.start();

        //
        // create large number of datastreams
        //
        for(int i = 0; i < concurrencyLevel; i++) {
            createDatastreamForDSM(zkClient, testCluster, testConectorType, datastreamName + i);
        }

        //
        // wait for the assignment to finish
        //
        Thread.sleep(waitDurationForZk * 2);

        //
        // verify all datastreams are assigned to the instance1
        //
        List<DatastreamTask> assigned = connector1.getTasks();
        Assert.assertEquals(assigned.size(), concurrencyLevel);

        instance1.stop();
        zkClient.close();
    }


    private void createDatastreamForDSM(ZkClient zkClient, String cluster, String connectorType, String datastreamName) {
        zkClient.ensurePath(KeyBuilder.datastreams(cluster));
        Datastream datastream = new Datastream();
        datastream.setName(datastreamName);
        datastream.setConnectorType(connectorType);
        String json = DatastreamJSonUtil.getJSonStringFromDatastream(datastream);
        zkClient.create(KeyBuilder.datastream(cluster, datastreamName), json, CreateMode.PERSISTENT);
    }
}