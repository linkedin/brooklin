package com.linkedin.datastream.common.zk;

import java.io.IOException;
import java.util.List;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.IZkDataListener;
import org.apache.zookeeper.CreateMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.datastream.common.PollUtils;

public class TestZkClient {
  private static final Logger logger = LoggerFactory.getLogger(TestZkClient.class.getName());

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
    String node0 = zkClient.create(electionNodeName, "test", CreateMode.PERSISTENT_SEQUENTIAL);
    String node1 = zkClient.create(electionNodeName, "content", CreateMode.PERSISTENT_SEQUENTIAL);
    Assert.assertEquals(node0, "/leaderelection/coordinator-0000000000");
    Assert.assertEquals(node1, "/leaderelection/coordinator-0000000001");

    List<String> liveNodes = zkClient.getChildren(electionPath, false);
    Assert.assertEquals(liveNodes.size(), 2);
    Assert.assertEquals(liveNodes.get(0), "coordinator-0000000000");
    Assert.assertEquals(liveNodes.get(1), "coordinator-0000000001");
    zkClient.close();

  }

  static class TestZkDataListener implements IZkDataListener, IZkChildListener {
    public boolean dataChanged = false;
    public boolean dataDeleted = false;
    public boolean childChanged = false;

    @Override
    public void handleDataChange(String s, Object o) throws Exception {
      dataChanged = true;
    }

    @Override
    public void handleDataDeleted(String s) throws Exception {
      // looks like when the node is deleted, this is triggered.
      dataDeleted = true;
    }

    @Override
    public void handleChildChange(String s, List<String> list) throws Exception {
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
  public void testRemoveTree() throws Exception {
    String [] paths = {
            "/a/b/c/d/e",
            "/a/b/c/d/f",
            "/a/b/c/g/j",
            "/a/b/c/y/j",
            "/a/b/m/k",
            "/a/b/m/n/p"};

    ZkClient client = new ZkClient(_zkConnectionString);

    for (String path  : paths) {
      client.ensurePath(path);
      Assert.assertTrue(client.exists(path));
    }

    client.removeTree("/a/b");
    for (String path  : paths) {
      Assert.assertFalse(client.exists(path));
    }

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
    zkClient.subscribeChildChanges("/", l);

    // now create the node
    zkClient2.create(znodePath, "", CreateMode.PERSISTENT);

    // child changed after new node created
    Assert.assertTrue(PollUtils.poll(() -> l.childChanged, 100, 1000));

    // now write some content to the node
    String textContent = "some content";
    zkClient2.writeData(znodePath, textContent);

    // now the data changed should have been called
    Assert.assertTrue(PollUtils.poll(() -> l.dataChanged, 100, 1000));

    String result = zkClient2.ensureReadData(znodePath);
    Assert.assertEquals(result, textContent);

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
    String instance1 = znodePath + "/" + "instance1";

    ZkClient zkClient1 = new ZkClient(_zkConnectionString);
    ZkClient zkClient2 = new ZkClient(_zkConnectionString);

    // create a live instance node for zkclient1
    zkClient1.create(znodePath, "", CreateMode.PERSISTENT);
    zkClient1.create(instance1, "", CreateMode.EPHEMERAL);

    // add data listener
    TestZkDataListener l = new TestZkDataListener();
    zkClient2.subscribeDataChanges(instance1, l);

    // test existance and setup watch
    Assert.assertTrue(PollUtils.poll(() -> zkClient2.exists(instance1, true), 100, 1000));

    // now disconnect zkClient1 so the node will disappear
    zkClient1.close();

    // test the node is gone
    Assert.assertTrue(PollUtils.poll(() -> !zkClient2.exists(instance1, true), 100, 1000));

    // verify the node disappear is registered in callbacks
    Assert.assertTrue(PollUtils.poll(() -> l.dataDeleted, 100, 1000));
    Assert.assertTrue(PollUtils.poll(() -> !l.dataChanged, 100, 1000));

    // what if the original live instance is temp offlien (GC pause)?
    zkClient1 = new ZkClient(_zkConnectionString);
    zkClient1.create(instance1, "text", CreateMode.EPHEMERAL);

    // verify the data is changed
    Assert.assertTrue(PollUtils.poll(() -> l.dataChanged, 100, 1000));

    zkClient1.close();
    zkClient2.close();
  }

  /**
   * make sure special symbols like dots can be in the znode names
   * @throws Exception
   */
  @Test
  public void testDotsInZnodeName() throws Exception {
    String path = "/testDotsInZnodeName";
    String path1 = path + "/yi.computer";
    ZkClient zkClient = new ZkClient(_zkConnectionString);
    zkClient.ensurePath(path);
    zkClient.create(path1, "abc", CreateMode.PERSISTENT);

    // read back
    String content = zkClient.readData(path1);
    Assert.assertEquals(content, "abc");

    zkClient.close();
  }
}
