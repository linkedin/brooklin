package com.linkedin.datastream.server.d2;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.d2.balancer.D2Client;
import com.linkedin.d2.balancer.D2ClientBuilder;
import com.linkedin.d2.balancer.Directory;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;


@Test(singleThreaded = true)
public class TestDatastreamD2Manager {
  private static final Logger LOG = LoggerFactory.getLogger(TestDatastreamD2Manager.class);

  private EmbeddedZookeeper _zookeeper;

  @BeforeTest
  public void setup() throws IOException {
    _zookeeper = new EmbeddedZookeeper();
    _zookeeper.startup();
  }

  @AfterTest
  public void teardown() throws IOException {
    if (_zookeeper != null) {
      _zookeeper.shutdown();
      _zookeeper = null;
    }
  }

  private DatastreamD2Manager createManager(String clusterName, String uri, BooleanSupplier isLeader) {
    Map<String, String> svcEndpoints = new HashMap<>();
    svcEndpoints.put("foo", "/foo");
    svcEndpoints.put("bar", "/bar");
    DatastreamD2Manager manager = new DatastreamD2Manager(
        _zookeeper.getConnection(),
        clusterName,
        uri,
        isLeader,
        new Properties(),
        svcEndpoints
    );
    return manager;
  }

  private void verifyD2Cluster(String clusterName, String... uris) throws Exception {
    String basePath = String.format("/%s/d2", clusterName);
    D2Client d2Client = new D2ClientBuilder()
        .setZkHosts(_zookeeper.getConnection())
        .setBasePath(basePath)
        .build();
    long timeoutMs = 30000;

    RestliUtils.callAsyncAndWait((f) -> d2Client.start(f), "start D2 client", timeoutMs, LOG);

    // Verify cluster structure
    Directory dir = d2Client.getFacilities().getDirectory();
    FutureCallback<List<String>> future1 = new FutureCallback<List<String>>() {
      @Override
      public void onSuccess(List<String> result) {
        try {
          Assert.assertEquals(result.size(), 1);
          Assert.assertEquals(result.get(0), "DatastreamService");
        } catch (AssertionError e) {
          super.onError(e);
        }
        super.onSuccess(result);
      }
    };
    dir.getClusterNames(future1);
    future1.get(timeoutMs, TimeUnit.MILLISECONDS);

    // Verify service endpoints
    FutureCallback<List<String>> future2 = new FutureCallback<List<String>>() {
      @Override
      public void onSuccess(List<String> result) {
        try {
          Assert.assertEquals(result.size(), 2);
          Assert.assertTrue(result.contains("foo"));
          Assert.assertTrue(result.contains("bar"));
        } catch (AssertionError e) {
          super.onError(e);
        }
        super.onSuccess(result);
      }
    };
    dir.getServiceNames(future2);
    future2.get(timeoutMs, TimeUnit.MILLISECONDS);

    // Verify URIs
    String uriRoot = String.format("/%s/d2/uris/%s", clusterName, DatastreamD2Config.DMS_D2_CLUSTER);
    ZkClient client = new ZkClient(_zookeeper.getConnection());
    List<String> children = client.getChildren(uriRoot, false);
    Assert.assertEquals(children.size(), uris.length);
    for (String node : children) {
      String json = client.ensureReadData(uriRoot + "/" + node);
      Assert.assertTrue(Arrays.stream(uris).filter(u -> json.contains(u)).findAny().isPresent(),
          "Unknown host in " + json);
    }
  }

  /**
   * Start one D2 server and verify the D2 cluster structure.
   * @throws Exception
   */
  @Test
  public void testSingleServerHappyPath() throws Exception {
    String clusterName = "testSingleServerHappyPath";
    String uri = "host1:123";
    DatastreamD2Manager d2Manager = createManager(clusterName, uri, () -> true);
    try {
      d2Manager.start();
      verifyD2Cluster(clusterName, uri);
    } finally {
      d2Manager.shutdown();
    }
  }

  /**
   * Start two D2 servers and verify the D2 cluster structure.
   * @throws Exception
   */
  @Test
  public void testTwoServersHappyPath() throws Exception {
    String clusterName = "testTwoServersHappyPath";
    String [] uris = { "host1:123", "host2:123" };
    DatastreamD2Manager d2Manager0 = createManager(clusterName, uris[0], () -> true);
    DatastreamD2Manager d2Manager1 = createManager(clusterName, uris[1], () -> true);

    try {
      d2Manager0.start();
      Thread.sleep(3000);
      d2Manager1.start();
      verifyD2Cluster(clusterName, uris);
    } finally {
      d2Manager1.shutdown();
      d2Manager0.shutdown();
    }
  }

  /**
   * Start two D2 servers, kill leader, and verify the D2 cluster structure.
   * @throws Exception
   */
  @Test
  public void testTwoServersLeaderOffline() throws Exception {
    String clusterName = "testTwoServersLeaderOffline";
    String [] uris = { "host1:123", "host2:123" };
    DatastreamD2Manager d2Manager0 = createManager(clusterName, uris[0], () -> true);
    DatastreamD2Manager d2Manager1 = createManager(clusterName, uris[1], () -> false);

    d2Manager0.start();
    d2Manager1.start();
    verifyD2Cluster(clusterName, uris);

    d2Manager0.shutdown();
    verifyD2Cluster(clusterName, uris[1]);

    d2Manager1.shutdown();
    verifyD2Cluster(clusterName);
  }

  /**
   * Start two D2 servers, kill follower, and verify the D2 cluster structure.
   * @throws Exception
   */
  @Test
  public void testTwoServersFollowerOffline() throws Exception {
    String clusterName = "testTwoServersFollowerOffline";
    String [] uris = { "host1:123", "host2:123" };
    DatastreamD2Manager d2Manager0 = createManager(clusterName, uris[0], () -> true);
    DatastreamD2Manager d2Manager1 = createManager(clusterName, uris[1], () -> false);

    d2Manager0.start();
    d2Manager1.start();
    verifyD2Cluster(clusterName, uris);

    d2Manager1.shutdown();
    verifyD2Cluster(clusterName, uris[0]);

    d2Manager0.shutdown();
    verifyD2Cluster(clusterName);
  }

  /**
   * Simulate the scenario two DSMs are started, follower DSM starts D2Manager before leader,
   * it should properly wait for the D2 cluster to get set up by leader before joinging.
   * Verify the D2 cluster structure in the end.
   * @throws Exception
   */
  @Test
  public void testTwoServersFollowerStartFirst() throws Exception {
    String clusterName = "testTwoServersFollowerStartFirst";
    String [] uris = { "host1:123", "host2:123" };
    DatastreamD2Manager d2Manager0 = createManager(clusterName, uris[0], () -> true);
    DatastreamD2Manager d2Manager1 = createManager(clusterName, uris[1], () -> false);

    ExecutorService executor = Executors.newSingleThreadExecutor();

    AtomicInteger stage = new AtomicInteger(0);
    executor.submit(() -> {
      stage.incrementAndGet();
      d2Manager1.start();
      stage.incrementAndGet();
    });

    Assert.assertTrue(PollUtils.poll(() -> stage.get() == 1, 100, 5000));

    d2Manager0.start();

    Assert.assertTrue(PollUtils.poll(() -> stage.get() == 2, 100, 5000));

    verifyD2Cluster(clusterName, uris);

    d2Manager1.shutdown();
    verifyD2Cluster(clusterName, uris[0]);

    d2Manager0.shutdown();
    verifyD2Cluster(clusterName);
  }
}
