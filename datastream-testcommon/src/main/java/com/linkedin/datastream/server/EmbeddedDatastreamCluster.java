package com.linkedin.datastream.server;

/*
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
 */

import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.NetworkUtils;
import com.linkedin.datastream.kafka.KafkaCluster;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;


public class EmbeddedDatastreamCluster {
  private static final Logger LOG = Logger.getLogger(EmbeddedDatastreamCluster.class);
  private static final String KAFKA_TRANSPORT_FACTORY = "com.linkedin.datastream.kafka.KafkaTransportProviderFactory";
  public static final String CONFIG_ZK_CONNECT = "zookeeper.connect";

  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

  // the zookeeper ports that are currently taken
  private final KafkaCluster _kafkaCluster;

  private int _numServers;
  private List<Integer> _datastreamPorts = new ArrayList<>();
  private List<Properties> _datastreamServerProperties = new ArrayList<>();
  private List<DatastreamServer> _servers = new ArrayList<>();

  private EmbeddedDatastreamCluster(Map<String, Properties> connectorProperties, Properties override,
      KafkaCluster kafkaCluster, int numServers)
      throws IOException {
    _kafkaCluster = kafkaCluster;
    _numServers = numServers;
    for (int i = 0; i < numServers; i++) {
      _servers.add(null);
      _datastreamServerProperties.add(null);
      _datastreamPorts.add(-1);
      setupDatastreamProperties(i, _kafkaCluster.getZkConnection(), connectorProperties, override, kafkaCluster);
    }
  }

  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override)
      throws IllegalArgumentException, IOException, DatastreamException {
    return newTestDatastreamCluster(kafkaCluster, connectorProperties, override, 1);
  }

  private void setupDatastreamProperties(int index, String zkConnectionString, Map<String, Properties> connectorProperties,
      Properties override, KafkaCluster kafkaCluster) {
    String connectorTypes = connectorProperties.keySet().stream().collect(Collectors.joining(","));
    Properties properties = new Properties();
    properties.put(DatastreamServer.CONFIG_CLUSTER_NAME, "DatastreamCluster");
    properties.put(DatastreamServer.CONFIG_ZK_ADDRESS, zkConnectionString);
    _datastreamPorts.set(index, NetworkUtils.getAvailablePort());
    properties.put(DatastreamServer.CONFIG_HTTP_PORT, String.valueOf(_datastreamPorts.get(index)));
    properties.put(DatastreamServer.CONFIG_CONNECTOR_TYPES, connectorTypes);
    properties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, KAFKA_TRANSPORT_FACTORY);
    properties.put(String.format("%s.kafka.%s", Coordinator.TRANSPORT_PROVIDER_CONFIG_DOMAIN, BOOTSTRAP_SERVERS_CONFIG),
        kafkaCluster.getBrokers());

    properties.put(String.format("%s.kafka.%s", Coordinator.TRANSPORT_PROVIDER_CONFIG_DOMAIN, CONFIG_ZK_CONNECT),
        kafkaCluster.getZkConnection());

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
        domainConnectorProperties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + connectorType + "." + propertyEntry,
            props.getProperty(propertyEntry));
      }
    }

    return domainConnectorProperties;
  }

  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override, int numServers)
      throws IllegalArgumentException, IOException, DatastreamException {

    EmbeddedDatastreamCluster cluster =
        new EmbeddedDatastreamCluster(connectorProperties, override, kafkaCluster, numServers);
    return cluster;
  }

  /**
   * Get the datastream server config for the primary datastream server
   * Configs for different servers in the cluster only differ by http port
   * number.
   */
  public Properties getPrimaryDatastreamServerProperties() {
    return _datastreamServerProperties.get(0);
  }

  /**
   * Get the http port for the primary datastream server
   */
  public int getPrimaryDatastreamPort() {
    return _datastreamPorts.get(0);
  }

  /**
   * Construct a datastream rest client for the primary datastream server
   */
  public DatastreamRestClient createDatastreamRestClient() {
    return createDatastreamRestClient(0);
  }

  /**
   * Construct a datastream rest client for the specific datastream server
   */
  public DatastreamRestClient createDatastreamRestClient(int index) {
    return new DatastreamRestClient(String.format("http://localhost:%d/", _datastreamPorts.get(index)));
  }

  public DatastreamServer getPrimaryDatastreamServer() {
    return _servers.get(0);
  }

  public List<DatastreamServer> getAllDatastreamServers() {
    return Collections.unmodifiableList(_servers);
  }

  public String getBrokerList() {
    if (_kafkaCluster == null) {
      LOG.error("kafka cluster not started correctly");
      return null;
    }

    return _kafkaCluster.getBrokers();
  }

  public String getZkConnection() {
    if (_kafkaCluster == null) {
      LOG.error("failed to get zookeeper connection string. Zookeeper service not started correctly.");
      return null;
    }

    return _kafkaCluster.getZkConnection();
  }

  private void prepareStartup()
      throws IOException {
    if (_kafkaCluster == null) {
      LOG.error("failed to start up kafka cluster: kafka cluster is not initialized correctly.");
      return;
    }

    if (!_kafkaCluster.isStarted()) {
      _kafkaCluster.startup();
    }
  }

  public void startupServer(int index)
      throws IOException, DatastreamException {
    Validate.isTrue(index >= 0, "Server index out of bound: " + index);
    if (index < _servers.size() && _servers.get(index) != null) {
      LOG.warn(String.format("Server[%d] already exists, skipping.", index));
      return;
    }

    prepareStartup();

    DatastreamServer server = new DatastreamServer(_datastreamServerProperties.get(index));
    _servers.set(index, server);
    server.startup();

    LOG.info(String.format("DatastreamServer[%d] started.", index));
  }

  public void startup()
      throws IOException, DatastreamException {
    int numServers = _numServers;
    for (int i = 0; i < numServers; i++) {
      startupServer(i);
    }
  }

  public void shutdownServer(int index) {
    Validate.isTrue(index >= 0 && index < _servers.size(), "Server index out of bound: " + index);
    if (_servers.get(index) == null) {
      LOG.warn(String.format("Server[%d] has not been initialized, skipping.", index));
      return;
    }
    _servers.get(index).shutdown();
    _servers.remove(index);
    if (_servers.size() == 0) {
      shutdown();
    }
  }

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
  }
}
