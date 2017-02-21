package com.linkedin.datastream.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.ThreadTerminationMonitor;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.ConnectorFactory;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;
import com.linkedin.datastream.server.dms.DatastreamResourceFactory;
import com.linkedin.datastream.server.dms.DatastreamResources;
import com.linkedin.datastream.server.dms.DatastreamStore;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;


/**
 * DatastreamServer is the entry point for starting datastream services. It is a container
 * for all datastream services including the rest api service, the coordinator and so on.
 */
public class DatastreamServer {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamServer.class);

  public static final String CONFIG_PREFIX = "brooklin.server.";
  public static final String CONFIG_CONNECTOR_NAMES = CONFIG_PREFIX + "connectorNames";
  public static final String CONFIG_HTTP_PORT = CONFIG_PREFIX + "httpPort";
  public static final String CONFIG_CSV_METRICS_DIR = CONFIG_PREFIX + "csvMetricsDir";
  public static final String CONFIG_ZK_ADDRESS = CoordinatorConfig.CONFIG_ZK_ADDRESS;
  public static final String CONFIG_CLUSTER_NAME = CoordinatorConfig.CONFIG_CLUSTER;
  public static final String CONFIG_FACTORY_CLASS_NAME = "factoryClassName";
  public static final String CONFIG_CONNECTOR_BOOTSTRAP_TYPE = "bootstrapConnector";
  public static final String CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY = "assignmentStrategyFactory";
  public static final String CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING = "customCheckpointing";
  public static final String CONFIG_CONNECTOR_PREFIX = CONFIG_PREFIX + "connector.";
  public static final String STRATEGY_DOMAIN = "strategy";
  public static final String CONFIG_TRANSPORT_PROVIDER_NAMES = CONFIG_PREFIX + "transportProviderNames";
  public static final String CONFIG_TRANSPORT_PROVIDER_PREFIX = CONFIG_PREFIX + "transportProvider.";

  private Coordinator _coordinator;
  private DatastreamStore _datastreamStore;
  private DatastreamNettyStandaloneLauncher _nettyLauncher;
  private JmxReporter _jmxReporter;

  private final String _csvMetricsDir;
  private final int _httpPort;
  private final Map<String, String> _bootstrapConnectors;

  private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
  private static final List<BrooklinMetricInfo> METRIC_INFOS = new ArrayList<>();

  private boolean _isInitialized = false;
  private boolean _isStarted = false;

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

  public List<BrooklinMetricInfo> getMetricInfos() {
    return Collections.unmodifiableList(METRIC_INFOS);
  }

  public DatastreamStore getDatastreamStore() {
    return _datastreamStore;
  }

  public int getHttpPort() {
    return _httpPort;
  }

  private void initializeTransportProvider(String transportProviderName, Properties transportProviderConfig) {
    LOG.info("Starting to load the transport provider: " + transportProviderName);

    String factoryClassName = transportProviderConfig.getProperty(CONFIG_FACTORY_CLASS_NAME, "");
    if (StringUtils.isBlank(factoryClassName)) {
      String msg = "Factory class name is not set or empty for transport provider: " + transportProviderName;
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    TransportProviderAdminFactory factory = ReflectionUtils.createInstance(factoryClassName);
    if (factory == null) {
      String msg = "Invalid class name or no parameter-less constructor, class=" + factoryClassName;
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    TransportProviderAdmin admin = factory.createTransportProviderAdmin(transportProviderName, transportProviderConfig);
    _coordinator.addTransportProvider(transportProviderName, admin);
  }

  private void initializeConnector(String connectorName, Properties connectorProperties) {
    LOG.info("Starting to load connector: " + connectorName);

    VerifiableProperties connectorProps = new VerifiableProperties(connectorProperties);

    // For each connector type defined in the config, load one instance from that class
    String className = connectorProperties.getProperty(CONFIG_FACTORY_CLASS_NAME, "");
    if (StringUtils.isBlank(className)) {
      String errorMessage = "Factory className is empty for connector " + connectorName;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    ConnectorFactory<?> connectorFactoryInstance = ReflectionUtils.createInstance(className);
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
    AssignmentStrategyFactory assignmentStrategyFactoryInstance = null;
    String strategyFactory = connectorProperties.getProperty(CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY, "");
    if (!strategyFactory.isEmpty()) {
      assignmentStrategyFactoryInstance = ReflectionUtils.createInstance(strategyFactory);
    }

    if (assignmentStrategyFactoryInstance == null) {
      String errorMessage = "Invalid strategy factory class: " + strategyFactory;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    Properties strategyProps = connectorProps.getDomainProperties(STRATEGY_DOMAIN);
    AssignmentStrategy assignmentStrategy = assignmentStrategyFactoryInstance.createStrategy(strategyProps);

    boolean customCheckpointing =
        Boolean.parseBoolean(connectorProperties.getProperty(CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING, "false"));
    _coordinator.addConnector(connectorName, connectorInstance, assignmentStrategy, customCheckpointing);

    LOG.info("Connector loaded successfully. Type: " + connectorName);
  }

  public DatastreamServer(Properties properties) throws DatastreamException {
    LOG.info("Start to initialize DatastreamServer. Properties: " + properties);
    LOG.info("Creating coordinator.");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    HashSet<String> connectorTypes =
        new HashSet<>(Arrays.asList(verifiableProperties.getString(CONFIG_CONNECTOR_NAMES).split(",")));
    if (connectorTypes.size() == 0) {
      String errorMessage = "No connectors specified in connectorTypes";
      LOG.error(errorMessage);
      throw new DatastreamRuntimeException(errorMessage);
    }

    HashSet<String> transportProviderNames =
        new HashSet<>(Arrays.asList(verifiableProperties.getString(CONFIG_TRANSPORT_PROVIDER_NAMES).split(",")));
    if (transportProviderNames.size() == 0) {
      String errorMessage = "No transport providers specified in config: " + CONFIG_TRANSPORT_PROVIDER_NAMES;
      LOG.error(errorMessage);
      throw new DatastreamRuntimeException(errorMessage);
    }

    CoordinatorConfig coordinatorConfig = new CoordinatorConfig(properties);
    coordinatorConfig.setAssignmentChangeThreadPoolThreadCount(connectorTypes.size());

    LOG.info("Setting up DMS endpoint server.");
    ZkClient zkClient = new ZkClient(coordinatorConfig.getZkAddress(), coordinatorConfig.getZkSessionTimeout(),
        coordinatorConfig.getZkConnectionTimeout());

    CachedDatastreamReader datastreamCache = new CachedDatastreamReader(zkClient, coordinatorConfig.getCluster());
    _coordinator = new Coordinator(datastreamCache, coordinatorConfig);
    LOG.info("Loading connectors.");
    _bootstrapConnectors = new HashMap<>();
    for (String connectorStr : connectorTypes) {
      initializeConnector(connectorStr,
          verifiableProperties.getDomainProperties(CONFIG_CONNECTOR_PREFIX + connectorStr));
    }

    LOG.info("Loading Transport providers.");
    for (String tpName : transportProviderNames) {
      initializeTransportProvider(tpName,
          verifiableProperties.getDomainProperties(CONFIG_TRANSPORT_PROVIDER_PREFIX + tpName));
    }

    _datastreamStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, coordinatorConfig.getCluster());
    _httpPort =
        verifiableProperties.getIntInRange(CONFIG_HTTP_PORT, 1024, 65535); // skipping well-known port range: (1~1023)
    _nettyLauncher = new DatastreamNettyStandaloneLauncher(_httpPort, new DatastreamResourceFactory(this),
        "com.linkedin.datastream.server.dms", "com.linkedin.datastream.server.diagnostics");

    _csvMetricsDir = verifiableProperties.getString(CONFIG_CSV_METRICS_DIR, "");

    verifiableProperties.verify();

    initializeMetrics();

    _isInitialized = true;

    LOG.info("DatastreamServer initialized successfully.");
  }

  private void initializeMetrics() {
    METRIC_INFOS.addAll(ThreadTerminationMonitor.getMetricInfos());
    METRIC_INFOS.addAll(_coordinator.getMetricInfos());
    METRIC_INFOS.addAll(DatastreamResources.getMetricInfos());

    _jmxReporter = JmxReporter.forRegistry(METRIC_REGISTRY).build();

    if (StringUtils.isNotEmpty(_csvMetricsDir)) {
      LOG.info("Starting CsvReporter in " + _csvMetricsDir);
      File csvDir = new File(_csvMetricsDir);
      if (!csvDir.exists()) {
        LOG.info(String.format("csvMetricsDir %s doesn't exist, creating it.", _csvMetricsDir));
        csvDir.mkdirs();
      }

      final CsvReporter reporter = CsvReporter.forRegistry(METRIC_REGISTRY)
          .formatFor(Locale.US)
          .convertRatesTo(SECONDS)
          .convertDurationsTo(MILLISECONDS)
          .build(csvDir);
      reporter.start(1, MINUTES);
    }
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

  private static Properties getServerProperties(String[] args) throws IOException {

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
