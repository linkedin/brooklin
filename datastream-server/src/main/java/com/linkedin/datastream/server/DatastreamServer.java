package com.linkedin.datastream.server;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.VerifiableProperties;

import com.linkedin.datastream.server.assignment.SimpleStrategy;
import com.linkedin.datastream.server.dms.DatastreamStore;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;
import com.linkedin.datastream.server.zk.ZkClient;
import com.linkedin.restli.server.NettyStandaloneLauncher;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


/**
 * DatastreamServer is the entry point for starting datastream services. It is a container
 * for all datastream services including the rest api service, the coordinator and so on.
 * DatastreamServer is designed to be singleton.
 */
public enum DatastreamServer {
  INSTANCE;

  public static final String CONFIG_PREFIX = "datastream.server.";
  public static final String CONFIG_CONNECTOR_TYPES = CONFIG_PREFIX + "connectorTypes";
  public static final String CONFIG_HTTP_PORT = CONFIG_PREFIX + "httpPort";
  public static final String CONFIG_EVENT_COLLECTOR_CLASS_NAME = DatastreamEventCollectorFactory.CONFIG_COLLECTOR_NAME;
  public static final String CONFIG_ZK_ADDRESS = CoordinatorConfig.CONFIG_ZK_ADDRESS;
  public static final String CONFIG_CLUSTER_NAME = CoordinatorConfig.CONFIG_CLUSTER;
  public static final String CONFIG_CONNECTOR_FACTORY_CLASS_NAME = "factoryClassName";
  public static final String CONFIG_CONNECTOR_BOOTSTRAP_TYPE = "bootstrapConnector";
  public static final String CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY = "assignmentStrategy";


  private static final Logger LOG = LoggerFactory.getLogger(DatastreamServer.class.getName());
  private static final ClassLoader _classLoader = DatastreamServer.class.getClassLoader();

  private Coordinator _coordinator;
  private DatastreamStore _datastreamStore;
  private NettyStandaloneLauncher _nettyLauncher;
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
      Class connectorFactoryClass = _classLoader.loadClass(className);
      ConnectorFactory connectorFactoryInstance;
      try {
        Constructor<ConnectorFactory> constructor = connectorFactoryClass.getConstructor();
        connectorFactoryInstance = constructor.newInstance();
      } catch (NoSuchMethodException e) {
        String msg = "No parameter-less constructor found for connector factory.";
        LOG.error(msg);
        throw new DatastreamException(msg, e);
      }

      Connector connectorInstance = connectorFactoryInstance.createConnector(connectorProperties);

      // Verify the connector type of the connector
      if (!connectorInstance.getConnectorType().equals(connectorStr)) {
        throw new DatastreamException(String.format("Wrong connector type for %s, expected: %s actual: %s",
            className, connectorStr, connectorInstance.getConnectorType()));
      }

      // Read the bootstrap connector type for the connector if there is one
      String bootstrapConnector = connectorProperties.getProperty(CONFIG_CONNECTOR_BOOTSTRAP_TYPE, "");
      if (!bootstrapConnector.isEmpty()) {
        _bootstrapConnectors.put(connectorStr, bootstrapConnector);
      }

      // Read the assignment strategy from the config; if not found, use default strategy
      AssignmentStrategy assignmentStrategyInstance;
      String strategy = connectorProperties.getProperty(CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, "");
      if (!strategy.isEmpty()) {
        Class assignmentStrategyClass = _classLoader.loadClass(strategy);
        assignmentStrategyInstance = (AssignmentStrategy) assignmentStrategyClass.newInstance();
      } else {
        assignmentStrategyInstance = new SimpleStrategy();
      }
      _coordinator.addConnector(connectorInstance, assignmentStrategyInstance);

    } catch (Exception ex) {
      LOG.error(String.format("Instantiating connector %s failed with exception", connectorStr, ex));
      throw new DatastreamException("Failed to instantiate connector: " + connectorStr, ex);
    }
    LOG.info("Connector loaded successfully. Type: " + connectorStr);
  }

  public synchronized void init(Properties properties) throws DatastreamException {
    if (isInitialized()) {
      LOG.warn("Attempt to initialize DatastreamServer while it is already initialized.");
      return;
    }
    LOG.info("Start to initialize DatastreamServer. Properties: " + properties);
    LOG.info("Creating coordinator.");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    CoordinatorConfig coordinatorConfig = new CoordinatorConfig(verifiableProperties);
    _coordinator = new Coordinator(coordinatorConfig);

    LOG.info("Loading connectors.");
    String connectorTypes = verifiableProperties.getString(CONFIG_CONNECTOR_TYPES);
    if (connectorTypes.isEmpty()) {
      throw new DatastreamException("No connectors specified in connectorTypes");
    }
    _bootstrapConnectors = new HashMap<>();
    for (String connectorStr : connectorTypes.split(",")) {
      initializeConnector(connectorStr, verifiableProperties.getDomainProperties(connectorStr));
    }

    LOG.info("Setting up DMS endpoint server.");
    ZkClient zkClient =
        new ZkClient(coordinatorConfig.getZkAddress(), coordinatorConfig.getZkSessionTimeout(),
            coordinatorConfig.getZkConnectionTimeout());
    _datastreamStore = new ZookeeperBackedDatastreamStore(zkClient, coordinatorConfig.getCluster());
    int httpPort = verifiableProperties.getIntInRange(CONFIG_HTTP_PORT, 1024, 65535); // skipping well-known port range: (1~1023)
    _nettyLauncher = new NettyStandaloneLauncher(httpPort, "com.linkedin.datastream.server.dms");

    try {
      _nettyLauncher.start();
    } catch (IOException ex) {
      throw new DatastreamException("Failed to start netty.", ex);
    }

    verifiableProperties.verify();
    _isInitialized = true;

    LOG.info("DatastreamServer initialized successfully.");
  }

  public synchronized void shutDown() {
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
}
