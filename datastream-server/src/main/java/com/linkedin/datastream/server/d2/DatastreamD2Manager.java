package com.linkedin.datastream.server.d2;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.common.callback.FutureCallback;
import com.linkedin.d2.balancer.config.PartitionDataFactory;
import com.linkedin.d2.balancer.servers.ZKUriStoreFactory;
import com.linkedin.d2.balancer.servers.ZooKeeperAnnouncer;
import com.linkedin.d2.balancer.servers.ZooKeeperConnectionManager;
import com.linkedin.d2.balancer.servers.ZooKeeperServer;
import com.linkedin.d2.discovery.util.D2Config;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.NetworkUtils;
import com.linkedin.datastream.common.RestliUtils;
import com.linkedin.datastream.common.zk.ZkClient;


/**
 * DatastreamD2Manager interacts with D2 to publish Datastream services and maintains a connection to the cluster.
 * This class should only be used when a datastream server starts up.
 */
public class DatastreamD2Manager {
  private static final String MODULE = DatastreamD2Manager.class.getName();
  private static final Logger LOG = LoggerFactory.getLogger(MODULE);

  private static final Duration ANNOUNCE_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration CLUSTER_WAIT_TIMEOUT = Duration.ofMinutes(5);
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofMinutes(5);

  private final String _zkConn;
  private final String _clusterName;
  private final String _uri;
  private final Properties _config;
  private final Map<String, String> _svcEndpoints;

  // Use lambda because leader status is volatile
  private final BooleanSupplier _isLeader;

  private DatastreamD2Config _d2Config;
  private ZooKeeperConnectionManager _connManager;
  private boolean _isStopped = true;

  /**
   * @param zkConn connection string to the ZooKeeper service
   * @param clusterName name of the Datastream service cluster
   * @param uri URI of the Datastream service host
   * @param isLeader a boolean supplier returning current leader status
   * @param config D2 config properties
   */
  public DatastreamD2Manager(String zkConn, String clusterName, String uri, BooleanSupplier isLeader,
      Properties config, Map<String, String> svcEndpoints) {
    Validate.isTrue(NetworkUtils.isValidUri(zkConn), "invalid ZK connection: " + zkConn);
    Validate.notBlank(clusterName, "invalid cluster name");
    Validate.isTrue(NetworkUtils.isValidUri(uri), "invalid server URI: " + uri);
    Validate.notNull(config, "null D2 config");
    Validate.notEmpty(svcEndpoints, "null or empty Restli service endpoints");

    _zkConn = zkConn;
    _clusterName = clusterName;
    _uri = StringUtils.prependIfMissing(uri, "http://");
    _isLeader = isLeader;
    _config = config;
    _svcEndpoints = svcEndpoints;

    if (isLeader.getAsBoolean()) {
      LOG.info("D2 leader is: " + uri);
    }
  }

  /**
   * Should be called only by the current leader instance of Datastream service
   * to set up D2 zookeeper cluster.
   *
   * @throws Exception
   */
  private void configureCluster() throws Exception {
    LOG.info(String.format("Begin configuring D2 cluster %s, leader=%s", _clusterName, _uri));

    D2Config d2Config = new D2Config(_zkConn,
        _d2Config.getSessionTimeout(),
        _d2Config.getZkBasePath(),
        _d2Config.getSessionTimeout(),
        _d2Config.getZkRetryLimit(),
        _d2Config.getClusterDefaults(),
        _d2Config.getServiceConfig(),
        _d2Config.getClusterConfig(),
        _d2Config.getExtraClusterConfig(),
        _d2Config.getServiceVariants());

    d2Config.configure();

    LOG.info("Finished configuring D2 cluster");
  }

  /**
   * Wait for D2 cluster in zookeeper to get fully populated.
   */
  private void waitForClusterSetup() {
    LOG.info("Waiting for D2 cluster set up by leader.");

    try {
      // Collect all ZK paths to be checked
      List<String> clusterDirs = new ArrayList<>();
      String d2Root = _d2Config.getZkBasePath();
      clusterDirs.add(d2Root);
      String svcRoot = d2Root + "/services/";
      _svcEndpoints.keySet().stream().map(ep -> svcRoot + ep).forEach(clusterDirs::add);
      clusterDirs.add(d2Root + "/clusters/" + DatastreamD2Config.DMS_D2_CLUSTER);

      ZkClient zkClient = new ZkClient(_zkConn);
      long timeoutMs = CLUSTER_WAIT_TIMEOUT.toMillis();
      clusterDirs.forEach(d -> zkClient.waitUntilExists(d, TimeUnit.MILLISECONDS, timeoutMs));
    } catch (Exception e) {
      String errMsg = "Timed out waiting for D2 cluster to set up";
      LOG.error(errMsg, e);
      throw new DatastreamRuntimeException(errMsg, e);
    }
  }

  /**
   * Join D2 cluster by announcing the Datastream service URI.
   * @throws Exception
   */
  private void joinCluster() throws Exception {
    ZooKeeperServer server = new ZooKeeperServer();
    ZooKeeperAnnouncer announcer = new ZooKeeperAnnouncer(server);
    announcer.setUri(_uri);
    announcer.setCluster(DatastreamD2Config.DMS_D2_CLUSTER);

    // DSM does not serve data hence has no partitioning
    Map<String, Object> partitionDataMap = new HashMap<>();
    partitionDataMap.put("0", DatastreamD2Config.getSingletonMap("weight", "1.0"));
    announcer.setWeightOrPartitionData(PartitionDataFactory.
        createPartitionDataMap(partitionDataMap));

    _connManager = new ZooKeeperConnectionManager(_zkConn,
        _d2Config.getSessionTimeout(),
        _d2Config.getZkBasePath(),
        new ZKUriStoreFactory(),
        _d2Config.getZkRetryLimit(),
        announcer);

    RestliUtils.callAsyncAndWait((f) -> _connManager.start(f), "start connection manager",
        ANNOUNCE_TIMEOUT.toMillis(), LOG);
  }

  public synchronized void start() {
    _d2Config = new DatastreamD2Config(_config, _clusterName, _svcEndpoints);

    if (_isLeader.getAsBoolean()) {
      try {
        configureCluster();
      } catch (Exception e) {
        String errMsg = "Failed to set up D2 cluster, server=" + this;
        LOG.error(errMsg, e);
        throw new DatastreamRuntimeException(errMsg, e);
      }
    }

    try {
      waitForClusterSetup();
      joinCluster();

      _isStopped = false;
    } catch (Exception e) {
      String errMsg = "Failed to join D2 cluster, server=" + this;
      LOG.error(errMsg, e);
      throw new DatastreamRuntimeException(errMsg, e);
    }
  }

  public synchronized void shutdownAsync() {
    if (_isStopped) {
      LOG.warn("already stopped: " + this);
      return;
    }
    _connManager.shutdown(new FutureCallback<>());
  }

  public synchronized void shutdown() {
    if (_isStopped) {
      LOG.warn("already stopped: " + this);
      return;
    }

    try {
      RestliUtils.callAsyncAndWait((f) -> _connManager.shutdown(f),
          "close D2 connection", SHUTDOWN_TIMEOUT.toMillis(), LOG);
    } finally {
      _isStopped = true;
    }
  }

  @Override
  public String toString() {
    return String.format("zkConn=%s, cluster=%s, uri=%s, leader=%s, endpoints=%s", _zkConn, _clusterName, _uri,
        _isLeader, _svcEndpoints);
  }

  public static String getZkBasePath(String clusterName) {
    return DatastreamD2Config.getZkBasePath(clusterName);
  }
}
