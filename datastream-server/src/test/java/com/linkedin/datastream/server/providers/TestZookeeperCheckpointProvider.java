package com.linkedin.datastream.server.providers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.TestDatastreamDestinationManager;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;


public class TestZookeeperCheckpointProvider {

  private EmbeddedZookeeper _zookeeper;

  @BeforeMethod
  public void setup()
      throws IOException {
    _zookeeper = new EmbeddedZookeeper();
    _zookeeper.startup();
  }

  @AfterMethod
  public void cleanup() {
    _zookeeper.shutdown();
  }

  @Test
  public void testCommit_AndReadCheckpoints() {
    ZkAdapter adapter = new ZkAdapter(_zookeeper.getConnection(), "testcluster");
    adapter.connect();
    ZookeeperCheckpointProvider checkpointProvider = new ZookeeperCheckpointProvider(adapter);
    Map<DatastreamTask, String> checkpoints = new HashMap<>();
    DatastreamTask datastreamTask1 = new DatastreamTask(TestDatastreamDestinationManager.generateDatastream(1));
    datastreamTask1.setId("dt1");
    checkpoints.put(datastreamTask1, "checkpoint1");

    DatastreamTask datastreamTask2 = new DatastreamTask(TestDatastreamDestinationManager.generateDatastream(2));
    datastreamTask2.setId("dt2");
    checkpoints.put(datastreamTask2, "checkpoint2");

    checkpointProvider.commit(checkpoints);
    List<DatastreamTask> tasks = new ArrayList<>();
    tasks.add(datastreamTask1);
    tasks.add(datastreamTask2);
    Map<DatastreamTask, String> commitedCheckpoints = checkpointProvider.committed(tasks);
    Assert.assertEquals(commitedCheckpoints.get(datastreamTask1), checkpoints.get(datastreamTask1));
    Assert.assertEquals(commitedCheckpoints.get(datastreamTask2), checkpoints.get(datastreamTask2));
  }
}
