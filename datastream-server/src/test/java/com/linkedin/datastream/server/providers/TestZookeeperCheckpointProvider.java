package com.linkedin.datastream.server.providers;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.DummyConnector;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.DummyTransportProviderAdminFactory;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;


public class TestZookeeperCheckpointProvider {

  private EmbeddedZookeeper _zookeeper;

  private String defaultTransportProviderName = "test";

  @BeforeMethod
  public void setup() throws IOException {
    DynamicMetricsManager.createInstance(new MetricRegistry());
    _zookeeper = new EmbeddedZookeeper();
    _zookeeper.startup();
  }

  @AfterMethod
  public void cleanup() {
    _zookeeper.shutdown();
  }

  public static Datastream generateDatastream(int seed) {
    Datastream ds = new Datastream();
    ds.setName("name_" + seed);
    ds.setConnectorName(DummyConnector.CONNECTOR_TYPE);
    ds.setSource(new DatastreamSource());
    ds.getSource().setConnectionString("DummySource_" + seed);
    ds.setDestination(new DatastreamDestination());
    ds.setTransportProviderName(DummyTransportProviderAdminFactory.PROVIDER_NAME);
    StringMap metadata = new StringMap();
    metadata.put("owner", "person_" + seed);
    ds.setMetadata(metadata);
    return ds;
  }

  @Test
  public void testUnassign() {
    ZkAdapter adapter = new ZkAdapter(_zookeeper.getConnection(), "testcluster", defaultTransportProviderName, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, null);
    adapter.connect();
    ZookeeperCheckpointProvider checkpointProvider = new ZookeeperCheckpointProvider(adapter);
    Datastream ds1 = generateDatastream(1);
    ds1.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    DatastreamTaskImpl datastreamTask1 = new DatastreamTaskImpl(Collections.singletonList(ds1));
    datastreamTask1.setId("dt1");

    Datastream ds2 = generateDatastream(2);
    ds2.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(ds1));
    DatastreamTaskImpl datastreamTask2 = new DatastreamTaskImpl(Collections.singletonList(ds2));
    datastreamTask2.setId("dt2");

    checkpointProvider.updateCheckpoint(datastreamTask1, 0, "checkpoint1");
    checkpointProvider.updateCheckpoint(datastreamTask2, 0, "checkpoint2");

    adapter.setDatastreamTaskStateForKey(datastreamTask1, ZookeeperCheckpointProvider.CHECKPOINT_KEY_NAME, "");
    checkpointProvider.unassignDatastreamTask(datastreamTask1);

    Map<Integer, String> commitedCheckpoints1 = checkpointProvider.getSafeCheckpoints(datastreamTask1);
    Assert.assertEquals(commitedCheckpoints1.size(), 0);

    Map<Integer, String> commitedCheckpoints2 = checkpointProvider.getSafeCheckpoints(datastreamTask2);
    Assert.assertEquals(commitedCheckpoints2.get(0), "checkpoint2");
  }

  @Test
  public void testCommitAndReadCheckpoints() {
    ZkAdapter adapter = new ZkAdapter(_zookeeper.getConnection(), "testcluster", defaultTransportProviderName, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, null);
    adapter.connect();
    ZookeeperCheckpointProvider checkpointProvider = new ZookeeperCheckpointProvider(adapter);
    DatastreamTaskImpl datastreamTask1 = new DatastreamTaskImpl(Collections.singletonList(generateDatastream(1)));
    datastreamTask1.setId("dt1");

    DatastreamTaskImpl datastreamTask2 = new DatastreamTaskImpl(Collections.singletonList(generateDatastream(2)));
    datastreamTask2.setId("dt2");

    checkpointProvider.updateCheckpoint(datastreamTask1, 0, "checkpoint1");
    checkpointProvider.updateCheckpoint(datastreamTask2, 0, "checkpoint2");

    Map<Integer, String> commitedCheckpoints1 = checkpointProvider.getSafeCheckpoints(datastreamTask1);
    Map<Integer, String> commitedCheckpoints2 = checkpointProvider.getSafeCheckpoints(datastreamTask2);
    Assert.assertEquals(commitedCheckpoints1.size(), 1);

    Assert.assertEquals(commitedCheckpoints1.get(0), "checkpoint1");
    Assert.assertEquals(commitedCheckpoints2.get(0), "checkpoint2");
  }

//  @Test
//  public void testReadCommitedShouldIncludeDatastreamTasksWhoseCheckpointsAreNotCommitted() {
//    ZkAdapter adapter = new ZkAdapter(_zookeeper.getConnection(), "testcluster", ZkClient.DEFAULT_SESSION_TIMEOUT,
//        ZkClient.DEFAULT_CONNECTION_TIMEOUT, null, null);
//    adapter.connect();
//    ZookeeperCheckpointProvider checkpointProvider = new ZookeeperCheckpointProvider(adapter);
//    DatastreamTaskImpl datastreamTask1 = new DatastreamTaskImpl(TestDestinationManager.generateDatastream(1));
//    datastreamTask1.setId("dt1");
//
//    DatastreamTaskImpl datastreamTask2 = new DatastreamTaskImpl(TestDestinationManager.generateDatastream(2));
//    datastreamTask2.setId("dt2");
//
//    List<DatastreamTask> tasks = new ArrayList<>();
//    tasks.add(datastreamTask1);
//    tasks.add(datastreamTask2);
//    Map<DatastreamTask, String> commitedCheckpoints = checkpointProvider.getCommitted(tasks);
//    Assert.assertEquals(commitedCheckpoints.get(datastreamTask1), checkpoints.get(datastreamTask1));
//    Assert.assertFalse(commitedCheckpoints.containsKey(datastreamTask2));
//  }
}
