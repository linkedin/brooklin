/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.Validate;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.DatastreamRestClientFactory;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.kafka.KafkaCluster;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;

/**
 * Provides a Datastream cluster, including a ZooKeeper cluster and Kafka cluster (if necessary), for testing purposes.
 */
public class EmbeddedDatastreamCluster {

  public static final String CONFIG_ZK_CONNECT = "zookeeper.connect";
  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

  private static final Logger LOG = LoggerFactory.getLogger(EmbeddedDatastreamCluster.class);
  private static final String KAFKA_TRANSPORT_FACTORY =
      "com.linkedin.datastream.kafka.KafkaTransportProviderAdminFactory";
  private static final long SERVER_INIT_TIMEOUT_MS = 60000; // 1 minute

  // ZooKeeper ports that are currently taken
  private final KafkaCluster _kafkaCluster;
  private final String _zkAddress;
  private EmbeddedZookeeper _zk = null;

  private int _numServers;

  private final List<Integer> _datastreamPorts = new ArrayList<>();
  private final List<Properties> _datastreamServerProperties = new ArrayList<>();
  private final List<DatastreamServer> _servers = new ArrayList<>();

  private EmbeddedDatastreamCluster(Map<String, Properties> connectorProperties, Properties override,
      KafkaCluster kafkaCluster, int numServers, @Nullable List<Integer> dmsPorts) throws IOException {
    _kafkaCluster = kafkaCluster;

    // If the datastream cluster doesn't use Kafka as transport, then we set up our own ZooKeeper; otherwise use
    // Kafka's ZooKeeper.
    if (_kafkaCluster != null) {
      _zkAddress = _kafkaCluster.getZkConnection();
    } else {
      _zk = new EmbeddedZookeeper();
      _zkAddress = _zk.getConnection();
    }

    _numServers = numServers;
    for (int i = 0; i < numServers; i++) {
      _servers.add(null);
      _datastreamServerProperties.add(null);
      int dmsPort = dmsPorts != null ? dmsPorts.get(i) : 0;
      setupDatastreamProperties(i, dmsPort, _zkAddress, connectorProperties, override, kafkaCluster);
      _datastreamPorts.add(dmsPort);
    }
  }

  /**
   * Create a new test datastream cluster
   * @param connectorProperties a map of the connector configs with connector name as the key
   * @param override any server level config override
   * @param numServers number of datastream servers in the cluster
   * @return a datastream cluster
   */
  public static EmbeddedDatastreamCluster newTestDatastreamCluster(Map<String, Properties> connectorProperties,
      Properties override, int numServers) throws IOException {
    return newTestDatastreamCluster(null, connectorProperties, override, numServers, null);
  }

  /**
   * Create a new test datastream cluster
   * @param kafkaCluster the Kafka cluster to start up
   * @param connectorProperties a map of the connector configs with connector name as the key
   * @param override any server level config override
   * @return a datastream cluster
   */
  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override) throws IOException {
    return newTestDatastreamCluster(kafkaCluster, connectorProperties, override, 1, null);
  }

  /**
   * Create a new test datastream cluster
   * @param connectorProperties a map of the connector configs with connector name as the key
   * @param override any server level config override
   * @return a datastream cluster
   */
  public static EmbeddedDatastreamCluster newTestDatastreamCluster(Map<String, Properties> connectorProperties,
      Properties override) throws IOException, DatastreamException {
    return newTestDatastreamCluster(null, connectorProperties, override);
  }

  /**
   * Create a new test datastream cluster
   * @param kafkaCluster Kafka cluster to be used by the datastream cluster
   * @param connectorProperties a map of the connector configs with connector name as the key
   * @param override any server level config override
   * @param numServers number of datastream servers in the cluster
   * @return a datastream cluster
   */
  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override, int numServers)
      throws IllegalArgumentException, IOException {
    return new EmbeddedDatastreamCluster(connectorProperties, override, kafkaCluster, numServers, null);
  }

  /**
   * Create a new test datastream cluster
   * @param kafkaCluster Kafka cluster to be used by the datastream cluster
   * @param connectorProperties a map of the connector configs with connector name as the key
   * @param override any server level config override
   * @param numServers number of datastream servers in the cluster
   * @param dmsPorts the dms ports to be used; accept null if automatic assignment
   * @return a datastream cluster
   */
  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override, int numServers,
      @Nullable List<Integer> dmsPorts) throws IllegalArgumentException, IOException {

    return new EmbeddedDatastreamCluster(connectorProperties, override, kafkaCluster, numServers, dmsPorts);
  }

  private void setupDatastreamProperties(int index, int httpPort, String zkConnectionString,
      Map<String, Properties> connectorProperties, Properties override, KafkaCluster kafkaCluster) {
    String connectorTypes = String.join(",", connectorProperties.keySet());
    Properties properties = new Properties();
    properties.put(CoordinatorConfig.CONFIG_HEARTBEAT_PERIOD_MS, String.valueOf(Duration.ofSeconds(1).toMillis()));
    properties.put(DatastreamServerConfigurationConstants.CONFIG_CLUSTER_NAME, "DatastreamCluster");
    properties.put(DatastreamServerConfigurationConstants.CONFIG_ZK_ADDRESS, zkConnectionString);
    properties.put(DatastreamServerConfigurationConstants.CONFIG_HTTP_PORT, String.valueOf(httpPort));
    properties.put(DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_NAMES, connectorTypes);
    String tpName = "default";
    String tpPrefix = DatastreamServerConfigurationConstants.CONFIG_TRANSPORT_PROVIDER_PREFIX + tpName + ".";
    properties.put(DatastreamServerConfigurationConstants.CONFIG_TRANSPORT_PROVIDER_NAMES, tpName);
    if (_kafkaCluster != null) {
      properties.put(tpPrefix + DatastreamServerConfigurationConstants.CONFIG_FACTORY_CLASS_NAME, KAFKA_TRANSPORT_FACTORY);
      properties.put(String.format("%s%s", tpPrefix, BOOTSTRAP_SERVERS_CONFIG), kafkaCluster.getBrokers());
      properties.put(String.format("%s%s", tpPrefix, CONFIG_ZK_CONNECT), kafkaCluster.getZkConnection());
    } else {
      properties.put(tpPrefix + DatastreamServerConfigurationConstants.CONFIG_FACTORY_CLASS_NAME,
          InMemoryTransportProviderAdminFactory.class.getTypeName());
    }

    properties.putAll(getDomainConnectorProperties(connectorProperties));
    if (override != null) {
      properties.putAll(override);
    }
    _datastreamServerProperties.set(index, properties);
  }

  private Properties getDomainConnectorProperties(Map<String, Properties> connectorProperties) {
    Properties domainConnectorProperties = new Properties();
    for (String connectorType : connectorProperties.keySet()) {
      Properties props = connectorProperties.get(connectorType);
      for (String propertyEntry : props.stringPropertyNames()) {
        domainConnectorProperties.put(DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_PREFIX + connectorType + "." + propertyEntry,
            props.getProperty(propertyEntry));
      }
    }

    return domainConnectorProperties;
  }

  public String getBrokers() {
    return _kafkaCluster.getBrokers();
  }

  public KafkaCluster getKafkaCluster() {
    return _kafkaCluster;
  }

  public int getNumServers() {
    return _numServers;
  }

  public List<Integer> getDatastreamPorts() {
    return _datastreamPorts;
  }

  public List<Properties> getDatastreamServerProperties() {
    return _datastreamServerProperties;
  }

  /**
   * Construct a datastream REST client for the primary datastream server
   */
  public DatastreamRestClient createDatastreamRestClient() {
    return createDatastreamRestClient(0);
  }

  /**
   * Construct a datastream REST client for the specific datastream server
   */
  public DatastreamRestClient createDatastreamRestClient(int index) {
    return DatastreamRestClientFactory.getClient(String.format("http://localhost:%d/", _datastreamPorts.get(index)));
  }

  public DatastreamServer getPrimaryDatastreamServer() {
    return _servers.get(0);
  }

  public List<DatastreamServer> getAllDatastreamServers() {
    return Collections.unmodifiableList(_servers);
  }

  public String getZkConnection() {
    return _zkAddress;
  }

  private void prepareStartup() throws IOException {
    if (_zk != null && !_zk.isStarted()) {
      _zk.startup();
    }

    if (_kafkaCluster != null && !_kafkaCluster.isStarted()) {
      _kafkaCluster.startup();
    }
  }

  /**
   * Start up the datastream server at the given {@code index}
   * @param index the index of the datastream server to start
   */
  public void startupServer(int index) throws IOException, DatastreamException {
    Validate.isTrue(index >= 0, "Server index out of bound: " + index);
    if (index < _servers.size() && _servers.get(index) != null) {
      LOG.warn("Server[{}] already exists, skipping.", index);
      return;
    }

    prepareStartup();

    DatastreamServer server = new DatastreamServer(_datastreamServerProperties.get(index));
    _servers.set(index, server);
    server.startup();

    // Update HTTP port in case it is lazily bound
    _datastreamPorts.set(index, server.getHttpPort());

    LOG.info("DatastreamServer[{}] started at port={}.", index, server.getHttpPort());
  }

  /**
   * Start up the datastream cluster
   */
  public void startup() throws IOException, DatastreamException {
    int numServers = _numServers;
    for (int i = 0; i < numServers; i++) {
      startupServer(i);
    }

    // Make sure all servers have started fully
    _servers.stream().forEach(server -> PollUtils.poll(server::isStarted, 1000, SERVER_INIT_TIMEOUT_MS));
  }

  /**
   * Shut down the datastream server at the given {@code index}
   * @param index the index of the datastream server to shut down
   */
  public void shutdownServer(int index) {
    Validate.isTrue(index >= 0 && index < _servers.size(), "Server index out of bound: " + index);
    if (_servers.get(index) == null) {
      LOG.warn("Server[{}] has not been initialized, skipping.", index);
      return;
    }
    _servers.get(index).shutdown();
    _servers.remove(index);
    if (_servers.size() == 0) {
      shutdown();
    }
  }

  /**
   * Shut down the datastream cluster
   */
  public void shutdown() {
    _servers.forEach(server -> {
      if (server != null && server.isStarted()) {
        server.shutdown();
      }
    });

    _servers.clear();

    if (_kafkaCluster != null) {
      _kafkaCluster.shutdown();
    }

    if (_zk != null) {
      _zk.shutdown();
    }
  }
}
