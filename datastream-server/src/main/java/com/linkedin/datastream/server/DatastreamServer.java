package com.linkedin.datastream.server;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DynamicMetricsManager;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.MetricsAware;
import com.linkedin.datastream.common.ReadOnlyMetricRegistry;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;
import com.linkedin.datastream.server.dms.BootstrapActionResources;
import com.linkedin.datastream.server.dms.DatastreamResourceFactory;
import com.linkedin.datastream.server.dms.DatastreamResources;
import com.linkedin.datastream.server.dms.DatastreamStore;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;

/**
 * DatastreamServer is the entry point for starting datastream services. It is a container
 * for all datastream services including the rest api service, the coordinator and so on.
 */
public class DatastreamServer {

  public static final String CONFIG_PREFIX = "datastream.server.";
  public static final String CONFIG_CONNECTOR_NAMES = CONFIG_PREFIX + "connectorNames";
  public static final String CONFIG_HTTP_PORT = CONFIG_PREFIX + "httpPort";
  public static final String CONFIG_ZK_ADDRESS = CoordinatorConfig.CONFIG_ZK_ADDRESS;
  public static final String CONFIG_CLUSTER_NAME = CoordinatorConfig.CONFIG_CLUSTER;
  public static final String CONFIG_TRANSPORT_PROVIDER_FACTORY = CoordinatorConfig.CONFIG_TRANSPORT_PROVIDER_FACTORY;
  public static final String CONFIG_CONNECTOR_FACTORY_CLASS_NAME = "factoryClassName";
  public static final String CONFIG_CONNECTOR_BOOTSTRAP_TYPE = "bootstrapConnector";
  public static final String CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY = "assignmentStrategy";
  public static final String CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING = "customCheckpointing";

  private static final Logger LOG = LoggerFactory.getLogger(DatastreamServer.class.getName());
  public static final String CONFIG_CONNECTOR_PREFIX = CONFIG_PREFIX + "connector.";

  private Coordinator _coordinator;
  private DatastreamStore _datastreamStore;
  private DatastreamNettyStandaloneLauncher _nettyLauncher;
  private boolean _isInitialized = false;
  private boolean _isStarted = false;

  private Map<String, String> _bootstrapConnectors;

  private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
  private static final Map<String, Metric> DYNAMIC_METRICS = new HashMap<>();
  private JmxReporter _jmxReporter;

  static {
    // Instantiate a dynamic metrics manager singleton object so that other components can emit metrics on the fly
    DynamicMetricsManager.createInstance(METRIC_REGISTRY);
  }

  public synchronized boolean isInitialized() {
    return _isInitialized;
  }

  public boolean isStarted() {
    return _isStarted;
  }

  public Coordinator getCoordinator() {
    return _coordinator;
  }

  public ReadOnlyMetricRegistry getMetricRegistry() {
    return new ReadOnlyMetricRegistry(METRIC_REGISTRY, DYNAMIC_METRICS);
  }

  public DatastreamStore getDatastreamStore() {
    return _datastreamStore;
  }

  private void initializeConnector(String connectorName, Properties connectorProperties) {
    LOG.info("Starting to load connector: " + connectorName);

    // For each connector type defined in the config, load one instance from that class
    String className = connectorProperties.getProperty(CONFIG_CONNECTOR_FACTORY_CLASS_NAME, "");
    if (StringUtils.isBlank(className)) {
      String errorMessage = "Factory className is empty for connector " + connectorName;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    ConnectorFactory connectorFactoryInstance = ReflectionUtils.createInstance(className);
    if (connectorFactoryInstance == null) {
      String msg = "Invalid class name or no parameter-less constructor, class=" + className;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, msg, null);
    }

    Connector connectorInstance = connectorFactoryInstance.createConnector(connectorName, connectorProperties);

    // Read the bootstrap connector type for the connector if there is one
    String bootstrapConnector = connectorProperties.getProperty(CONFIG_CONNECTOR_BOOTSTRAP_TYPE, "");
    if (!bootstrapConnector.isEmpty()) {
      _bootstrapConnectors.put(connectorName, bootstrapConnector);
    }

    // Read the assignment strategy from the config; if not found, use default strategy
    AssignmentStrategy assignmentStrategyInstance = null;
    String strategy = connectorProperties.getProperty(CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY, "");
    if (!strategy.isEmpty()) {
      assignmentStrategyInstance = ReflectionUtils.createInstance(strategy);
      if (assignmentStrategyInstance == null) {
        String errorMessage = "Invalid strategy class: " + strategy;
        ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
      }
    }

    boolean customCheckpointing =
        Boolean.parseBoolean(connectorProperties.getProperty(CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING, "false"));
    _coordinator.addConnector(connectorName, connectorInstance, assignmentStrategyInstance, customCheckpointing);

    LOG.info("Connector loaded successfully. Type: " + connectorName);
  }

  public DatastreamServer(Properties properties) throws DatastreamException {
    if (isInitialized()) {
      LOG.warn("Attempt to initialize DatastreamServer while it is already initialized.");
      return;
    }
    LOG.info("Start to initialize DatastreamServer. Properties: " + properties);
    LOG.info("Creating coordinator.");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    String[] connectorTypes = verifiableProperties.getString(CONFIG_CONNECTOR_NAMES).split(",");
    if (connectorTypes.length == 0) {
      String errorMessage = "No connectors specified in connectorTypes";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    CoordinatorConfig coordinatorConfig = new CoordinatorConfig(properties);
    coordinatorConfig.setAssignmentChangeThreadPoolThreadCount(connectorTypes.length);


    LOG.info("Setting up DMS endpoint server.");
    ZkClient zkClient =
        new ZkClient(coordinatorConfig.getZkAddress(), coordinatorConfig.getZkSessionTimeout(),
            coordinatorConfig.getZkConnectionTimeout());

    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(zkClient, coordinatorConfig.getCluster());
    _coordinator = new Coordinator(datastreamCache, coordinatorConfig);
    LOG.info("Loading connectors.");
    _bootstrapConnectors = new HashMap<>();
    for (String connectorStr : connectorTypes) {
      initializeConnector(connectorStr, verifiableProperties.getDomainProperties(CONFIG_CONNECTOR_PREFIX + connectorStr));
    }

    _datastreamStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, coordinatorConfig.getCluster());
    int httpPort = verifiableProperties.getIntInRange(CONFIG_HTTP_PORT, 1024, 65535); // skipping well-known port range: (1~1023)
    _nettyLauncher = new DatastreamNettyStandaloneLauncher(httpPort, new DatastreamResourceFactory(this),
        "com.linkedin.datastream.server.dms", "com.linkedin.datastream.server.diagnostics");

    verifiableProperties.verify();

    initializeMetrics();

    _isInitialized = true;

    LOG.info("DatastreamServer initialized successfully.");
  }

  private void initializeMetrics() {
    registerMetrics(_coordinator.getMetrics());
    registerMetrics(DatastreamResources.getMetrics());
    registerMetrics(BootstrapActionResources.getMetrics());

    _jmxReporter = JmxReporter.forRegistry(METRIC_REGISTRY).build();
  }

  private void registerMetrics(Map<String, Metric> metrics) {
    Optional.of(metrics).ifPresent(m -> m.forEach((key, value) -> {
      try {
        // If the key is a regular expression, the metric is dynamic, in which case it should be registered
        // later, after the server starts. Only the "static" metrics need to be registered before the server starts.
        if (key.contains(MetricsAware.KEY_REGEX)) {
          DYNAMIC_METRICS.put(key, value);
        } else {
          METRIC_REGISTRY.register(key, value);
        }
      } catch (IllegalArgumentException e) {
        LOG.warn("Metric " + key + " has already been registered.", e);
      }
    }));
  }

  public synchronized void startup() throws DatastreamException {
    // Start the JMX reporter
    if (_jmxReporter != null) {
      _jmxReporter.start();
    }

    // Start the coordinator
    if (_coordinator != null) {
      _coordinator.start();
    }

    // Start the DMS rest endpoint.
    try {
      _nettyLauncher.start();
      _isStarted = true;
    } catch (IOException ex) {
      String errorMessage = "Failed to start netty.";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, ex);
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

    if (_jmxReporter != null) {
      _jmxReporter.stop();
    }
    _isInitialized = false;
    _isStarted = false;
  }

  public static void main(String[] args) throws Exception {
    Properties serverProperties = getServerProperties(args);
    DatastreamServer server = new DatastreamServer(serverProperties);
    ReentrantLock lock = new ReentrantLock();
    Condition shutdownCondition = lock.newCondition();
    // attach shutdown handler to catch control-c
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        lock.lock();
        LOG.info("Starting the shutdown process..");
        server.shutdown();
        shutdownCondition.signalAll();
      }
    });

    lock.lock();
    server.startup();
    shutdownCondition.await();
    LOG.info("Main thread is exiting...");
  }

  private static Properties getServerProperties(String[] args)
      throws IOException {

    if (args.length == 0) {
      System.err.println(
          String.format("USAGE: java [options] %s server.properties ", DatastreamServer.class.getSimpleName()));
    }

    return loadProps(args[0]);
  }

  public static Properties loadProps(String filename) throws IOException {
    Properties props = new Properties();

    try (InputStream propStream = new FileInputStream(filename)) {
      props.load(propStream);
    }

    return props;
  }
}
