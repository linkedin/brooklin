package com.linkedin.datastream.server;

import java.io.IOException;
import java.util.List;

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
        List<DatastreamTask> _tasks;

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

    // Test that the broadcast strategy will be able to assign one task. The difference between this test
    // with testConnectorOnAssignmentChange is that this test will create the original datastream task
    // under zookeeper /{cluster}/datastreams, and verify that the assignment strategy is indeeded triggered
    // and the datastreamtask data structure is copied under the instance node.
    @Test
    public void testAssignmentBasic() throws Exception {

        String testCluster = "testAssignmentBasic";
        String testConectorType = "testConnectorType";
        String datastreamName = "datastream1";

        Coordinator instance1 = new Coordinator(_zkConnectionString, testCluster);
        TestHookConnector connector1 = new TestHookConnector(testConectorType);
        instance1.addConnector(connector1, new BroadcastStrategy());
        instance1.start();

        ZkClient zkClient = new ZkClient(_zkConnectionString);

        //
        // create datastream definitions under /testAssignmentBasic/datastream/datastream1
        //
        Datastream datastream = new Datastream();
        datastream.setName(datastreamName);
        datastream.setConnectorType(testConectorType);
        String json = DatastreamJSonUtil.getJSonStringFromDatastream(datastream);

        zkClient.ensurePath(KeyBuilder.datastreams(testCluster));
        zkClient.create(KeyBuilder.datastream(testCluster, datastreamName), json, CreateMode.PERSISTENT);

        //
        // wait for assignment to be done
        //
        Thread.sleep(1000);

        // verify the instance has 1 task assigned
        List<DatastreamTask> assigned1 = connector1.getTasks();
        Assert.assertEquals(assigned1.size(), 1);

        instance1.stop();
        zkClient.close();
    }
}