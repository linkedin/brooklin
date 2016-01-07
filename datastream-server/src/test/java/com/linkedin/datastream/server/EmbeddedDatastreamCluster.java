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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.log4j.Logger;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.kafka.KafkaTransportProvider;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;
import com.linkedin.datastream.testutil.TestUtils;


public class EmbeddedDatastreamCluster {
  private static final Logger LOG = Logger.getLogger(EmbeddedDatastreamCluster.class);
  private static final Lock MUTEX = new ReentrantLock(true);
  private static final String KAFKA_TRANSPORT_FACTORY = "com.linkedin.datastream.kafka.KafkaTransportProviderFactory";

  // the zookeeper ports that are currently taken
  private static Set<Integer> _zkPorts = new HashSet<>();

  private EmbeddedZookeeper _embeddedZookeeper = null;
  private EmbeddedKafkaCluster _embeddedKafkaCluster = null;
  private int _datastreamPort;
  private Properties _datastreamServerProperties;
  private DatastreamServer _server;

  private EmbeddedDatastreamCluster(Map<String, Properties> connectorProperties, Properties override, int zkPort)
      throws IOException {
    _embeddedZookeeper = new EmbeddedZookeeper(zkPort);
    List<Integer> kafkaPorts = new ArrayList<>();
    // -1 for any available port
    kafkaPorts.add(-1);
    kafkaPorts.add(-1);
    _embeddedKafkaCluster = new EmbeddedKafkaCluster(_embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
    setupDatastreamProperties(_embeddedZookeeper.getConnection(), connectorProperties, override);
  }

  public static EmbeddedDatastreamCluster newTestDatastreamKafkaCluster(Map<String, Properties> connectorProperties,
      Properties override) throws IllegalArgumentException, IOException, DatastreamException {
    return newTestDatastreamKafkaCluster(connectorProperties, override, -1);
  }

  private void setupDatastreamProperties(String zkConnectionString, Map<String, Properties> connectorProperties,
      Properties override) {
    String connectorTypes = connectorProperties.keySet().stream().collect(Collectors.joining(","));
    Properties properties = new Properties();
    properties.put(DatastreamServer.CONFIG_CLUSTER_NAME, "DatastreamCluster");
    properties.put(DatastreamServer.CONFIG_ZK_ADDRESS, zkConnectionString);
    _datastreamPort = TestUtils.getAvailablePort();
    properties.put(DatastreamServer.CONFIG_HTTP_PORT, String.valueOf(_datastreamPort));
    properties.put(DatastreamServer.CONFIG_CONNECTOR_TYPES, connectorTypes);
    properties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, KAFKA_TRANSPORT_FACTORY);
    properties.put(
        String.format("%s.%s", Coordinator.TRANSPORT_PROVIDER_CONFIG_DOMAIN, ProducerConfig.BOOTSTRAP_SERVERS_CONFIG),
        _embeddedKafkaCluster.getBrokers());

    properties.put(
        String.format("%s.%s", Coordinator.TRANSPORT_PROVIDER_CONFIG_DOMAIN, KafkaTransportProvider.CONFIG_ZK_CONNECT),
        _embeddedKafkaCluster.getZkConnection());

    properties.putAll(getDomainConnectorProperties(connectorProperties));
    if (override != null) {
      properties.putAll(override);
    }
    _datastreamServerProperties = properties;
  }

  private Properties getDomainConnectorProperties(Map<String, Properties> connectorProperties) {
    Properties domainConnectorProperties = new Properties();
    for (String connectorType : connectorProperties.keySet()) {
      Properties props = connectorProperties.get(connectorType);
      for (String propertyEntry : props.stringPropertyNames()) {
        domainConnectorProperties.put(connectorType + "." + propertyEntry, props.getProperty(propertyEntry));
      }
    }

    return domainConnectorProperties;
  }

  public static EmbeddedDatastreamCluster newTestDatastreamKafkaCluster(Map<String, Properties> connectorProperties,
      Properties override, int zkPort) throws IllegalArgumentException, IOException, DatastreamException {

    MUTEX.lock();

    if (zkPort != -1 && _zkPorts.contains(new Integer(zkPort))) {
      throw new IllegalArgumentException("This zookeeper port " + zkPort + " is taken. Choose another port");
    }

    _zkPorts.add(zkPort);

    EmbeddedDatastreamCluster cluster = new EmbeddedDatastreamCluster(connectorProperties, override, zkPort);

    MUTEX.unlock();

    return cluster;
  }

  public Properties getDatastreamServerProperties() {
    return _datastreamServerProperties;
  }

  public int getDatastreamPort() {
    return _datastreamPort;
  }

  public DatastreamServer getDatastreamServer() {
    return _server;
  }

  public String getBrokerList() {
    if (_embeddedKafkaCluster == null) {
      LOG.error("kafka cluster not started correctly");
      return null;
    }

    return _embeddedKafkaCluster.getBrokers();
  }

  public String getZkConnection() {
    if (_embeddedZookeeper == null) {
      LOG.error("failed to get zookeeper connection string. Zookeeper service not started correctly.");
      return null;
    }

    return _embeddedZookeeper.getConnection();
  }

  public void startup() throws IOException, DatastreamException {
    if (_embeddedKafkaCluster == null || _embeddedZookeeper == null) {
      LOG.error("failed to start up kafka cluster: either kafka or zookeeper service is not initialized correctly.");
      return;
    }

    _embeddedZookeeper.startup();
    _embeddedKafkaCluster.startup();
    _server = new DatastreamServer(_datastreamServerProperties);
    _server.startup();
  }

  public void shutdown() {
    _server.shutdown();

    if (_embeddedKafkaCluster != null) {
      _embeddedKafkaCluster.shutdown();
    }

    if (_embeddedZookeeper != null) {
      _embeddedZookeeper.shutdown();
    }

    // make this zookeeper port available
    MUTEX.lock();
    _zkPorts.remove(_embeddedZookeeper.getPort());
    MUTEX.unlock();
  }
}
