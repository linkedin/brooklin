package com.linkedin.datastream.server.zk;

/*
 *
 * Copyright 2015 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
*/

import java.io.IOException;
import java.util.List;

import org.apache.log4j.Logger;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.testutil.EmbeddedZookeeper;

public class TestCoordinatorZkAdapter {
    private static final Logger logger = Logger.getLogger(TestCoordinatorZkAdapter.class.getName());

    EmbeddedZookeeper _embeddedZookeeper;
    String _zkConnectionString;

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

        CoordinatorZkAdapter adapter1 = new CoordinatorZkAdapter(_zkConnectionString, testCluster);
        CoordinatorZkAdapter adapter2 = new CoordinatorZkAdapter(_zkConnectionString, testCluster);

        // verify the zookeeper path exists for the two live instance nodes
        ZkClient client = new ZkClient(_zkConnectionString);

        List<String> instances = client.getChildren(KeyBuilder.instances(testCluster));

        Assert.assertEquals(instances.size(), 2);

        client.close();
    }

    @Test
    public void testLeaderElection() throws Exception {
        String testCluster = "test_adapter_leader";

        CoordinatorZkAdapter adapter1 = new CoordinatorZkAdapter(_zkConnectionString, testCluster, 1000, 15000);
        CoordinatorZkAdapter adapter2 = new CoordinatorZkAdapter(_zkConnectionString, testCluster, 1000, 15000);

        Assert.assertTrue(adapter1.isLeader());
        Assert.assertFalse(adapter2.isLeader());

        // disconnect the first adapter to simulate it going offline
        adapter1.disconnect();
        Thread.sleep(1500);
        // adapter2 should now be the new leader
        Assert.assertTrue(adapter2.isLeader());

        // the adapter2 also go offline
        adapter2.disconnect();
        // now a new client goes online
        CoordinatorZkAdapter adapter3 = new CoordinatorZkAdapter(_zkConnectionString, testCluster, 1000, 15000);
        Thread.sleep(1500);
        Assert.assertTrue(adapter3.isLeader());
    }
}