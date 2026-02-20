/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.io.IOException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;

import org.mockito.Mockito;
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
import com.linkedin.datastream.server.zk.KeyBuilder;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Tests for {@link ZookeeperCheckpointProvider}
 */
public class TestZookeeperCheckpointProvider {

  private EmbeddedZookeeper _zookeeper;
  private ZkClient _zkClient;

  private final String defaultTransportProviderName = "test";
  private static final long DEBOUNCE_TIMER_MS = 1000;
  private static final long LOG_SIZE_LIMIT_IN_BYTES = 1024 * 1024;

  @BeforeMethod
  public void setup(Method method) throws IOException {
    DynamicMetricsManager.createInstance(new MetricRegistry(), method.getName());
    _zookeeper = new EmbeddedZookeeper();
    _zookeeper.startup();
    _zkClient = new ZkClient(_zookeeper.getConnection());
  }

  @AfterMethod
  public void cleanup() {
    _zkClient.close();
    _zookeeper.shutdown();
  }

  @Test
  public void testUnassign() {
    ZkAdapter adapter = new ZkAdapter(_zookeeper.getConnection(), "testcluster", defaultTransportProviderName, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, DEBOUNCE_TIMER_MS, LOG_SIZE_LIMIT_IN_BYTES, null);
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

    _zkClient.ensurePath(KeyBuilder.connectorTask("testcluster", ds1.getConnectorName(), datastreamTask1.getDatastreamTaskName()));
    _zkClient.ensurePath(KeyBuilder.connectorTask("testcluster", ds2.getConnectorName(), datastreamTask2.getDatastreamTaskName()));

    checkpointProvider.updateCheckpoint(datastreamTask1, 0, "checkpoint1");
    checkpointProvider.updateCheckpoint(datastreamTask2, 0, "checkpoint2");

    adapter.setDatastreamTaskStateForKey(datastreamTask1, ZookeeperCheckpointProvider.CHECKPOINT_KEY_NAME, "");
    checkpointProvider.unassignDatastreamTask(datastreamTask1);

    Map<Integer, String> committedCheckpoints1 = checkpointProvider.getSafeCheckpoints(datastreamTask1);
    Assert.assertEquals(committedCheckpoints1.size(), 0);

    Map<Integer, String> committedCheckpoints2 = checkpointProvider.getSafeCheckpoints(datastreamTask2);
    Assert.assertEquals(committedCheckpoints2.get(0), "checkpoint2");
  }

  @Test
  public void testCommitAndReadCheckpoints() {
    ZkAdapter adapter = new ZkAdapter(_zookeeper.getConnection(), "testcluster", defaultTransportProviderName, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, DEBOUNCE_TIMER_MS, LOG_SIZE_LIMIT_IN_BYTES, null);
    adapter.connect();
    ZookeeperCheckpointProvider checkpointProvider = new ZookeeperCheckpointProvider(adapter);
    DatastreamTaskImpl datastreamTask1 = new DatastreamTaskImpl(Collections.singletonList(generateDatastream(1)));
    datastreamTask1.setId("dt1");

    DatastreamTaskImpl datastreamTask2 = new DatastreamTaskImpl(Collections.singletonList(generateDatastream(2)));
    datastreamTask2.setId("dt2");

    _zkClient.ensurePath(KeyBuilder.connectorTask("testcluster", datastreamTask1.getConnectorType(), datastreamTask1.getDatastreamTaskName()));
    _zkClient.ensurePath(KeyBuilder.connectorTask("testcluster", datastreamTask2.getConnectorType(), datastreamTask2.getDatastreamTaskName()));

    checkpointProvider.updateCheckpoint(datastreamTask1, 0, "checkpoint1");
    checkpointProvider.updateCheckpoint(datastreamTask2, 0, "checkpoint2");

    Map<Integer, String> committedCheckpoints1 = checkpointProvider.getSafeCheckpoints(datastreamTask1);
    Map<Integer, String> committedCheckpoints2 = checkpointProvider.getSafeCheckpoints(datastreamTask2);
    Assert.assertEquals(committedCheckpoints1.size(), 1);

    Assert.assertEquals(committedCheckpoints1.get(0), "checkpoint1");
    Assert.assertEquals(committedCheckpoints2.get(0), "checkpoint2");
  }

  @Test
  public void testFlush() {
    ZkAdapter adapter = spy(new ZkAdapter(_zookeeper.getConnection(), "testcluster", defaultTransportProviderName, ZkClient.DEFAULT_SESSION_TIMEOUT,
        ZkClient.DEFAULT_CONNECTION_TIMEOUT, null));
    adapter.connect();
    ZookeeperCheckpointProvider checkpointProvider = new ZookeeperCheckpointProvider(adapter);
    DatastreamTaskImpl datastreamTask1 = new DatastreamTaskImpl(Collections.singletonList(generateDatastream(1)));
    datastreamTask1.setId("dt1");

    DatastreamTaskImpl datastreamTask2 = new DatastreamTaskImpl(Collections.singletonList(generateDatastream(2)));
    datastreamTask2.setId("dt2");

    checkpointProvider.updateCheckpoint(datastreamTask1, 0, "checkpoint1");
    checkpointProvider.updateCheckpoint(datastreamTask2, 0, "checkpoint2");

    Map<Integer, String> committedCheckpoints1 = checkpointProvider.getSafeCheckpoints(datastreamTask1);
    Map<Integer, String> committedCheckpoints2 = checkpointProvider.getSafeCheckpoints(datastreamTask2);
    Assert.assertEquals(committedCheckpoints1.size(), 1);

    Assert.assertEquals(committedCheckpoints1.get(0), "checkpoint1");
    Assert.assertEquals(committedCheckpoints2.get(0), "checkpoint2");

    verify(adapter, times(2)).setDatastreamTaskStateForKey(any(), anyString(), anyString());
    Mockito.reset(adapter);
    checkpointProvider.flush();
    verify(adapter, times(0)).setDatastreamTaskStateForKey(any(), anyString(), anyString());
    checkpointProvider.updateCheckpoint(datastreamTask1, 0, "checkpoint3");
    Mockito.reset(adapter);
    checkpointProvider.flush();
    verify(adapter, times(1)).setDatastreamTaskStateForKey(any(), anyString(), anyString());
  }

  /**
   * Generate a datastream
   */
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
}
