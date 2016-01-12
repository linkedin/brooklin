package com.linkedin.datastream.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;
import com.linkedin.datastream.server.assignment.SimpleStrategy;
import com.linkedin.datastream.server.dms.DatastreamResourceFactory;
import com.linkedin.datastream.server.dms.DatastreamStore;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;

/**
 * DatastreamServer is the entry point for starting datastream services. It is a container
 * for all datastream services including the rest api service, the coordinator and so on.
 */
public class DatastreamServer {

  public static final String CONFIG_PREFIX = "datastream.server.";
  public static final String CONFIG_CONNECTOR_TYPES = CONFIG_PREFIX + "connectorTypes";
  public static final String CONFIG_HTTP_PORT = CONFIG_PREFIX + "httpPort";
  public static final String CONFIG_ZK_ADDRESS = CoordinatorConfig.CONFIG_ZK_ADDRESS;
  public static final String CONFIG_CLUSTER_NAME = CoordinatorConfig.CONFIG_CLUSTER;
  public static final String CONFIG_TRANSPORT_PROVIDER_FACTORY = CoordinatorConfig.CONFIG_TRANSPORT_PROVIDER_FACTORY;
  public static final String CONFIG_CONNECTOR_FACTORY_CLASS_NAME = "factoryClassName";
  public static final String CONFIG_CONNECTOR_BOOTSTRAP_TYPE = "bootstrapConnector";
  public static final String CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY = "assignmentStrategy";
  public static final String CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING = "customCheckpointing";

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamServer.class.getName());
  private static final String CONNECTOR_CONFIG_PREFIX = "datastream.server.connector.";

  private Coordinator _coordinator;
  private DatastreamStore _datastreamStore;
  private DatastreamNettyStandaloneLauncher _nettyLauncher;
  private boolean _isInitialized = false;

  private Map<String, String> _bootstrapConnectors;

  public synchronized boolean isInitialized() {
    return _isInitialized;
  }

  public Coordinator getCoordinator() {
    return _coordinator;
  }

  public DatastreamStore getDatastreamStore() {
    return _datastreamStore;
  }

  private void initializeConnector(String connectorStr, Properties connectorProperties) throws DatastreamException {
    LOG.info("Starting to load connector: " + connectorStr);
    try {
      // For each connector type defined in the config, load one instance from that class
      String className = connectorProperties.getProperty(CONFIG_CONNECTOR_FACTORY_CLASS_NAME, "");
      if (StringUtils.isBlank(className)) {
        throw new DatastreamException("Factory className is empty for connector " + connectorStr);
      }
      ConnectorFactory connectorFactoryInstance = ReflectionUtils.createInstance(className);
      if (connectorFactoryInstance == null) {
        String msg = "Invalid class name or no parameter-less constructor, class=" + className;
        LOG.error(msg);
        throw new DatastreamException(msg);
      }

      Connector connectorInstance = connectorFactoryInstance.createConnector(connectorProperties);

      // Read the bootstrap connector type for the connector if there is one
      String bootstrapConnector = connectorProperties.getProperty(CONFIG_CONNECTOR_BOOTSTRAP_TYPE, "");
      if (!bootstrapConnector.isEmpty()) {
        _bootstrapConnectors.put(connectorStr, bootstrapConnector);
      }

      // Read the assignment strategy from the config; if not found, use default strategy
      AssignmentStrategy assignmentStrategyInstance = null;
      String strategy = connectorProperties.getProperty(CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, "");
      if (!strategy.isEmpty()) {
        assignmentStrategyInstance = ReflectionUtils.createInstance(strategy);
        if (assignmentStrategyInstance == null) {
          LOG.warn("Invalid strategy class: " + strategy);
        }
      }

      if (assignmentStrategyInstance == null) {
        assignmentStrategyInstance = new SimpleStrategy();
      }

      boolean customCheckpointing =
          Boolean.parseBoolean(connectorProperties.getProperty(CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING, "false"));
      _coordinator.addConnector(connectorStr, connectorInstance, assignmentStrategyInstance, customCheckpointing);

    } catch (Exception ex) {
      LOG.error(String.format("Instantiating connector %s failed with exception", connectorStr, ex));
      throw new DatastreamException("Failed to instantiate connector: " + connectorStr, ex);
    }
    LOG.info("Connector loaded successfully. Type: " + connectorStr);
  }

  public DatastreamServer(Properties properties) throws DatastreamException {
    if (isInitialized()) {
      LOG.warn("Attempt to initialize DatastreamServer while it is already initialized.");
      return;
    }
    LOG.info("Start to initialize DatastreamServer. Properties: " + properties);
    LOG.info("Creating coordinator.");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    String[] connectorTypes = verifiableProperties.getString(CONFIG_CONNECTOR_TYPES).split(",");
    if (connectorTypes.length == 0) {
      throw new DatastreamException("No connectors specified in connectorTypes");
    }

    CoordinatorConfig coordinatorConfig = new CoordinatorConfig(properties);
    coordinatorConfig.setAssignmentChangeThreadPoolThreadCount(connectorTypes.length);
    _coordinator = new Coordinator(coordinatorConfig);
    LOG.info("Loading connectors.");
    _bootstrapConnectors = new HashMap<>();
    for (String connectorStr : connectorTypes) {
      initializeConnector(connectorStr, verifiableProperties.getDomainProperties(CONNECTOR_CONFIG_PREFIX + connectorStr));
    }

    LOG.info("Setting up DMS endpoint server.");
    ZkClient zkClient =
        new ZkClient(coordinatorConfig.getZkAddress(), coordinatorConfig.getZkSessionTimeout(),
            coordinatorConfig.getZkConnectionTimeout());
    _datastreamStore = new ZookeeperBackedDatastreamStore(zkClient, coordinatorConfig.getCluster());
    int httpPort = verifiableProperties.getIntInRange(CONFIG_HTTP_PORT, 1024, 65535); // skipping well-known port range: (1~1023)
    _nettyLauncher = new DatastreamNettyStandaloneLauncher(httpPort, new DatastreamResourceFactory(this),
        "com.linkedin.datastream.server.dms", "com.linkedin.datastream.server.diagnostics");

    verifiableProperties.verify();
    _isInitialized = true;

    LOG.info("DatastreamServer initialized successfully.");
  }

  public synchronized void startup() throws DatastreamException {
    // Start the coordinator
    if (_coordinator != null) {
      _coordinator.start();
    }

    // Start the DMS rest endpoint.
    try {
      _nettyLauncher.start();
    } catch (IOException ex) {
      throw new DatastreamException("Failed to start netty.", ex);
    }
  }

  public synchronized void shutdown() {
    if (_coordinator != null) {
      _coordinator.stop();
      _coordinator = null;
    }
    _datastreamStore = null;
    if (_nettyLauncher != null) {
      try {
        _nettyLauncher.stop();
      } catch (IOException e) {
        LOG.error("Fail to stop netty launcher.", e);
      }
      _nettyLauncher = null;
    }
    _isInitialized = false;
  }

  /**
   * Return the corresponding bootstrap connector type for a connector
   * @param baseConnectorType the base (online) connector type
   * @return bootstrap connector type for the base connector. If the base connector doesn't
   * have a bootstrap connector, a DatastreamException will be thrown.
   * @throws DatastreamException
   */
  public String getBootstrapConnector(String baseConnectorType) throws DatastreamException {
    if (!_isInitialized) {
      throw new DatastreamException("DatastreamServer is not initialized.");
    }
    String ret = _bootstrapConnectors.get(baseConnectorType);
    if (ret == null) {
      throw new DatastreamException("No bootstrap connector specified for connector: " + baseConnectorType);
    }
    return ret;
  }

  public static void main(String[] args) throws Exception {
    Properties serverProperties = getServerProperties(args);
    DatastreamServer server = new DatastreamServer(serverProperties);
    Thread mainThread = Thread.currentThread();
    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        server.shutdown();
        mainThread.notifyAll();
      }
    });

    server.startup();
    mainThread.wait();
  }

  private static Properties getServerProperties(String[] args)
      throws IOException {

    if (args.length == 0) {
      System.err.println(
          "USAGE: java [options] %s server.properties ".format(DatastreamServer.class.getSimpleName()));
    }

    return loadProps(args[0]);
  }

  public static Properties loadProps(String filename) throws IOException {
    Properties props = new Properties();
    InputStream propStream = null;
    try {
      propStream = new FileInputStream(filename);
      props.load(propStream);
    } finally {
      if (propStream != null)
        propStream.close();
    }
    return props;
  }
}
