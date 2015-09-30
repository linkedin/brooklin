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
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.server.zk.ZkClient;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import org.apache.zookeeper.CreateMode;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestDatastreamContextImpl {
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

  @Test
  public void testDatastreamContextSmoke() throws Exception {
    String testCluster = "testDatastreamContextState";
    String connectorType = "connectorType";
    ZkAdapter adapter = new ZkAdapter(_zkConnectionString, testCluster);
    adapter.connect();

    DatastreamContext context = new DatastreamContextImpl(adapter);

    Datastream ds = new Datastream();
    ds.setName("datastream1");
    ds.setConnectorType(connectorType);

    DatastreamTask task1 = new DatastreamTask(ds);

    //
    // validate an undefined state key will return null
    //
    String value = context.getState(task1, "undefined");
    Assert.assertNull(value);

    //
    // save some states with key value pair
    //
    context.saveState(task1, "partition", "0");
    context.saveState(task1, "binlog", "binlog.000130");

    //
    // retrieve state and verify the value are expected
    //
    value = context.getState(task1, "partition");
    Assert.assertEquals(value, "0");

    value = context.getState(task1, "binlog");
    Assert.assertEquals(value, "binlog.000130");

    //
    // cleanup
    //
    adapter.disconnect();
  }
}
