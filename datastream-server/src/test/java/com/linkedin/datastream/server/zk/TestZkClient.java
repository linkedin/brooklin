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

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.IZkStateListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.testutil.EmbeddedZookeeper;

public class TestZkClient {
  private static final Logger logger = Logger.getLogger(TestZkClient.class.getName());

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
  public void testReadAndWriteRoundTrip() throws Exception {
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    String rootZNode = "/datastream";

    // make sure this node does not exist
    Assert.assertFalse(zkClient.exists(rootZNode, false));

    // now create this node with persistent mode
    zkClient.create(rootZNode, "", CreateMode.PERSISTENT);

    // verify the node is created and exists now
    Assert.assertTrue(zkClient.exists(rootZNode, false));

    zkClient.close(); 
  }

  @Test
  public void testCreateEphemeralSequentialNode() throws Exception {
    ZkClient zkClient = new ZkClient(_zkConnectionString);

    String electionPath = "/leaderelection";
    String electionNodeName = electionPath + "/coordinator-";

    // now create this node with persistent mode
    zkClient.create(electionPath, "", CreateMode.PERSISTENT);

    // now create this node with persistent mode
    String node0 = zkClient.create(electionNodeName, "", CreateMode.PERSISTENT_SEQUENTIAL);
    String node1 = zkClient.create(electionNodeName, "", CreateMode.PERSISTENT_SEQUENTIAL);
    Assert.assertEquals(node0, "/leaderelection/coordinator-0000000000");
    Assert.assertEquals(node1, "/leaderelection/coordinator-0000000001");

    List<String> liveNodes = zkClient.getChildren(electionPath, false);
    Assert.assertEquals(liveNodes.size(), 2);
    Assert.assertEquals(liveNodes.get(0), "coordinator-0000000000");
    Assert.assertEquals(liveNodes.get(1), "coordinator-0000000001");
    zkClient.close();

  }

  static class TestZkDataListener implements  IZkDataListener, IZkStateListener, IZkChildListener {
    public boolean dataChanged = false;
    public boolean dataDeleted = false;
    public boolean stateChanged = false;
    public boolean newSession = false;
    public boolean childChanged = false;

    @Override
    public void handleDataChange(String s, Object o)
        throws Exception {
      dataChanged = true;
    }

    @Override
    public void handleDataDeleted(String s)
        throws Exception {
      // looks like when the node is deleted, this is triggered.
      dataDeleted = true;
    }

    @Override
    public void handleStateChanged(Watcher.Event.KeeperState keeperState)
        throws Exception {
      stateChanged = true;
    }

    @Override
    public void handleNewSession()
        throws Exception {
      newSession = true;
    }

    @Override
    public void handleChildChange(String s, List<String> list)
        throws Exception {
      childChanged = true;
    }
  }

  @Test
  public void testEnsurePath() throws Exception {
    String path1 = "/datastreamtest/espresso/instances";

    ZkClient client = new ZkClient(_zkConnectionString);

    // make sure the paths don't exist
    Assert.assertFalse(client.exists(path1));

    // call ensurePath
    client.ensurePath(path1);

    // make sure both paths now exist
    Assert.assertTrue(client.exists(path1));

    client.close();
  }

  @Test
  public void testZNodeDataChange() throws Exception {
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    ZkClient zkClient2 = new ZkClient(_zkConnectionString);

    String znodePath = "/znode1";

    // make sure this node does not exist
    Assert.assertFalse(zkClient.exists(znodePath, false));

    TestZkDataListener l = new TestZkDataListener();
    zkClient.subscribeDataChanges(znodePath, l);
    zkClient.subscribeStateChanges(l);
    zkClient.subscribeChildChanges("/", l);

    // now create the node
    zkClient2.create(znodePath, "", CreateMode.PERSISTENT);

    // wait for a second so the callback can finish
    Thread.sleep(1000);

    // child changed after new node created
    Assert.assertTrue(l.childChanged);

    // now write some content to the node
    zkClient2.writeData(znodePath, "come text");

    // wait for a second so the callback can finish
    Thread.sleep(1000);

    // now the data changed should have been called
    Assert.assertTrue(l.dataChanged);

    zkClient.close();
    zkClient2.close();
  }

  /**
   * This is to validate the live instance situation, when a live instance
   * goes offline, the node will be deleted, and handleDataDeleted callback
   * will be triggered
   *
   * @throws Exception
   */
  @Test
  public void testLiveNode() throws Exception {

    String znodePath = "/instances";
    String instance1 = znodePath + "/" +  "instance1";

    ZkClient zkClient1 = new ZkClient(_zkConnectionString);
    ZkClient zkClient2 = new ZkClient(_zkConnectionString);

    // create a live instance node for zkclient1
    zkClient1.create(znodePath, "", CreateMode.PERSISTENT);
    zkClient1.create(instance1, "", CreateMode.EPHEMERAL);

    // add data listener
    TestZkDataListener l = new TestZkDataListener();
    zkClient2.subscribeDataChanges(instance1, l);
    zkClient2.subscribeStateChanges(l);

    // test existance and setup watch
    Thread.sleep(1000);
    boolean instance1live = zkClient2.exists(instance1, true);
    Assert.assertTrue(instance1live);

    // now disconnect zkClient1 so the node will disappear
    zkClient1.close();

    // wait for the ephemeral node to disappear
    Thread.sleep(1000);

    // test the node is gong
    instance1live = zkClient2.exists(instance1);
    Assert.assertFalse(instance1live);

    // verify the node disappear is registered in callbacks
    Assert.assertTrue(l.dataDeleted);
    Assert.assertFalse(l.dataChanged);

    // what if the original live instance is temp offlien (GC pause)?
    zkClient1 = new ZkClient(_zkConnectionString);
    zkClient1.create(instance1, "", CreateMode.EPHEMERAL);

    Thread.sleep(1000);

    // verify the data is changed
    Assert.assertTrue(l.dataChanged);

    zkClient1.close();
    zkClient2.close();
  }
}