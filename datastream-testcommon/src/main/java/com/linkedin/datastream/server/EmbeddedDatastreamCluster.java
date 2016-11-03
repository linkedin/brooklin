package com.linkedin.datastream.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.Nullable;

import com.linkedin.datastream.DatastreamRestClient;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.NetworkUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.kafka.KafkaCluster;
import com.linkedin.datastream.testutil.EmbeddedZookeeper;


public class EmbeddedDatastreamCluster {
  private static final Logger LOG = Logger.getLogger(EmbeddedDatastreamCluster.class);
  private static final String KAFKA_TRANSPORT_FACTORY = "com.linkedin.datastream.kafka.KafkaTransportProviderFactory";
  private static final long SERVER_INIT_TIMEOUT_MS = 60000; // 1 minute
  public static final String CONFIG_ZK_CONNECT = "zookeeper.connect";
  public static final String BOOTSTRAP_SERVERS_CONFIG = "bootstrap.servers";

  // the zookeeper ports that are currently taken
  private final KafkaCluster _kafkaCluster;
  private final String _zkAddress;
  private EmbeddedZookeeper _zk = null;

  private int _numServers;

  private List<Integer> _datastreamPorts = new ArrayList<>();
  private List<Properties> _datastreamServerProperties = new ArrayList<>();
  private List<DatastreamServer> _servers = new ArrayList<>();

  private EmbeddedDatastreamCluster(Map<String, Properties> connectorProperties, Properties override,
      KafkaCluster kafkaCluster, int numServers, @Nullable List<Integer> dmsPorts) throws IOException {
    _kafkaCluster = kafkaCluster;

    // If the datastream cluster doesn't use the Kafka as transport, then we setup our own zookeeper otherwise
    // use the kafka's zookeeper.
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
      int dmsPort = -1;
      if (dmsPorts != null) {
        dmsPort = dmsPorts.get(i);
      }
      _datastreamPorts.add(dmsPort);
      setupDatastreamProperties(i, _zkAddress, connectorProperties, override, kafkaCluster);
    }
  }

  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override) throws IOException, DatastreamException {
    return newTestDatastreamCluster(kafkaCluster, connectorProperties, override, 1, null);
  }

  public static EmbeddedDatastreamCluster newTestDatastreamCluster(Map<String, Properties> connectorProperties,
      Properties override) throws IOException, DatastreamException {
    return newTestDatastreamCluster(null, connectorProperties, override);
  }

  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override, int numServers)
      throws IllegalArgumentException, IOException, DatastreamException {

    return new EmbeddedDatastreamCluster(connectorProperties, override, kafkaCluster, numServers, null);
  }

  private void setupDatastreamProperties(int index, String zkConnectionString,
      Map<String, Properties> connectorProperties, Properties override, KafkaCluster kafkaCluster) {
    String connectorTypes = connectorProperties.keySet().stream().collect(Collectors.joining(","));
    Properties properties = new Properties();
    properties.put(DatastreamServer.CONFIG_CLUSTER_NAME, "DatastreamCluster");
    properties.put(DatastreamServer.CONFIG_ZK_ADDRESS, zkConnectionString);
    if (_datastreamPorts.get(index) == -1) {
      _datastreamPorts.set(index, NetworkUtils.getAvailablePort());
    }
    properties.put(DatastreamServer.CONFIG_HTTP_PORT, String.valueOf(_datastreamPorts.get(index)));
    properties.put(DatastreamServer.CONFIG_CONNECTOR_NAMES, connectorTypes);

    if (_kafkaCluster != null) {
      properties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY, KAFKA_TRANSPORT_FACTORY);
      properties.put(
          String.format("%s.%s", Coordinator.TRANSPORT_PROVIDER_CONFIG_DOMAIN, BOOTSTRAP_SERVERS_CONFIG),
          kafkaCluster.getBrokers());
      properties.put(String.format("%s.%s", Coordinator.TRANSPORT_PROVIDER_CONFIG_DOMAIN, CONFIG_ZK_CONNECT),
          kafkaCluster.getZkConnection());
    } else {
      properties.put(DatastreamServer.CONFIG_TRANSPORT_PROVIDER_FACTORY,
          InMemoryTransportProviderFactory.class.getTypeName());
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
        domainConnectorProperties.put(DatastreamServer.CONFIG_CONNECTOR_PREFIX + connectorType + "." + propertyEntry,
            props.getProperty(propertyEntry));
      }
    }

    return domainConnectorProperties;
  }

  /**
   * Create a new test datastream cluster
   * @param kafkaCluster kafka cluster to be used by the datastream cluster
   * @param connectorProperties a map of the connector configs with connector name as the keys
   * @param override any server level config override
   * @param numServers number of datastream servers in the cluster
   * @param dmsPorts the dms ports to be used; accept null if automatic assignment
   * @return instance of a new EmbeddedDatastreamCluster
   * @throws IllegalArgumentException
   * @throws IOException
   * @throws DatastreamException
   */
  public static EmbeddedDatastreamCluster newTestDatastreamCluster(KafkaCluster kafkaCluster,
      Map<String, Properties> connectorProperties, Properties override, int numServers,
      @Nullable List<Integer> dmsPorts) throws IllegalArgumentException, IOException, DatastreamException {

    return new EmbeddedDatastreamCluster(connectorProperties, override, kafkaCluster, numServers, dmsPorts);
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

  public void startupServer(int index) throws IOException, DatastreamException {
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

  public void startup() throws IOException, DatastreamException {
    int numServers = _numServers;
    for (int i = 0; i < numServers; i++) {
      startupServer(i);
    }

    // Make sure all servers have started fully
    _servers.stream().forEach(s -> PollUtils.poll(() -> s.isStarted(), 1000, SERVER_INIT_TIMEOUT_MS));

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

    if (_zk != null) {
      _zk.shutdown();
    }
  }
}
