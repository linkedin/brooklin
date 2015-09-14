package com.linkedin.datastream.server.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.datastream.server.DatastreamTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.datastream.common.Datastream;

public class TestZkAdapter {
    private static final Logger logger = LoggerFactory.getLogger(TestZkAdapter.class.getName());

    EmbeddedZookeeper _embeddedZookeeper;
    String _zkConnectionString;
    private static final int _zkWaitInMs = 500;

    @BeforeMethod
    public void setup() throws IOException {
        // each embeddedzookeeper should be on different port
        // so the tests can run in parallel
        _embeddedZookeeper = new EmbeddedZookeeper();
        _zkConnectionString = _embeddedZookeeper.getConnection();
        _embeddedZookeeper.startup();
    }

    @AfterMethod
    public void teardown() throws IOException {
        _embeddedZookeeper.shutdown();
    }

    @Test
    public void testSmoke() throws Exception {
        String testCluster = "test_adapter_smoke";

        ZkAdapter adapter1 = new ZkAdapter(_zkConnectionString, testCluster);
        adapter1.connect();

        ZkAdapter adapter2 = new ZkAdapter(_zkConnectionString, testCluster);
        adapter2.connect();

        // verify the zookeeper path exists for the two live instance nodes
        ZkClient client = new ZkClient(_zkConnectionString);

        List<String> instances = client.getChildren(KeyBuilder.instances(testCluster));

        Assert.assertEquals(instances.size(), 2);

        adapter1.disconnect();
        adapter2.disconnect();

        client.close();
    }

    @Test
    public void testLeaderElection() throws Exception {
        String testCluster = "test_adapter_leader";

        //
        // start two ZkAdapters, which is corresponding to two Coordinator instances
        //
        ZkAdapter adapter1 = new ZkAdapter(_zkConnectionString, testCluster, 1000, 15000);
        adapter1.connect();

        ZkAdapter adapter2 = new ZkAdapter(_zkConnectionString, testCluster, 1000, 15000);
        adapter2.connect();

        //
        // verify the first one started is the leader, and the second one is a follower
        //
        Assert.assertTrue(adapter1.isLeader());
        Assert.assertFalse(adapter2.isLeader());

        //
        // disconnect the first adapter to simulate it going offline
        //
        adapter1.disconnect();

        //
        // wait for leadership election to happen
        //
        Thread.sleep(_zkWaitInMs);

        //
        // adapter2 should now be the new leader
        //
        Assert.assertTrue(adapter2.isLeader());

        //
        // adapter2 goes offline, but new instance adapter2 goes online
        //
        adapter2.disconnect();
        // now a new client goes online
        ZkAdapter adapter3 = new ZkAdapter(_zkConnectionString, testCluster, 1000, 15000);
        adapter3.connect();
        Thread.sleep(_zkWaitInMs);

        //
        // verify that the adapter3 is the current leader
        //
        Assert.assertTrue(adapter3.isLeader());
    }

    @Test
    public void testZkBasedDatastreamList() throws Exception {
        String testCluster = "testZkBasedDatastreamList";

        ZkClient zkClient = new ZkClient(_zkConnectionString);

        ZkAdapter adapter1 = new ZkAdapter(_zkConnectionString, testCluster);
        adapter1.connect();

        // create new datastreams in zookeeper
        zkClient.create(KeyBuilder.datastream(testCluster, "stream1"), "stream1", CreateMode.PERSISTENT);
        Thread.sleep(_zkWaitInMs);

        List<Datastream> streams = adapter1.getAllDatastreams();
        Assert.assertEquals(streams.size(), 1);

        // create new datastreams in zookeeper
        zkClient.create(KeyBuilder.datastream(testCluster, "stream2"), "stream1", CreateMode.PERSISTENT);
        Thread.sleep(_zkWaitInMs);
        streams = adapter1.getAllDatastreams();
        Assert.assertEquals(streams.size(), 2);

        adapter1.disconnect();
        zkClient.close();
    }

    // When Coordinator leader writes the assignment to a specific instance, the change is indeed
    // persisted in zookeeper
    @Test
    public void testUpdateInstanceAssignment() throws Exception {
        String testCluster = "testUpdateInstanceAssignment";

        ZkClient zkClient = new ZkClient(_zkConnectionString);

        ZkAdapter adapter = new ZkAdapter(_zkConnectionString, testCluster);
        adapter.connect();

        List<DatastreamTask> tasks = new ArrayList<>();

        DatastreamTask task1 = new DatastreamTask();
        task1.setDatastreamName("task1");
        task1.setConnectorType("connectorType");
        tasks.add(task1);

        adapter.updateInstanceAssignment(adapter.getInstanceName(), tasks);
        List<String> assignment = zkClient.getChildren(KeyBuilder.instance(testCluster, adapter.getInstanceName()));
        Assert.assertEquals(assignment.size(), 1);

        // now add a new assignment
        // assignment = [task1, task2]
        DatastreamTask task2 = new DatastreamTask();
        task1.setDatastreamName("task2");
        task1.setConnectorType("connectorType");
        tasks.add(task2);

        adapter.updateInstanceAssignment(adapter.getInstanceName(), tasks);
        assignment = zkClient.getChildren(KeyBuilder.instance(testCluster, adapter.getInstanceName()));
        Assert.assertEquals(assignment.size(), 2);

        // assignment = [task1, task3]
        DatastreamTask task3 = new DatastreamTask();
        task1.setDatastreamName("task3");
        task1.setConnectorType("connectorType");
        tasks.add(task3);

        tasks.add(task3);
        tasks.remove(task2);
        adapter.updateInstanceAssignment(adapter.getInstanceName(), tasks);
        assignment = zkClient.getChildren(KeyBuilder.instance(testCluster, adapter.getInstanceName()));
        Assert.assertEquals(assignment.size(), 2);

        zkClient.close();
    }
}