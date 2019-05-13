/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
import com.linkedin.datastream.server.api.connector.DatastreamDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamDeduperFactory;
import com.linkedin.datastream.server.api.serde.SerdeAdmin;
import com.linkedin.datastream.server.api.serde.SerdeAdminFactory;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategy;
import com.linkedin.datastream.server.api.strategy.AssignmentStrategyFactory;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;
import com.linkedin.datastream.server.diagnostics.ServerComponentHealthAggregator;
import com.linkedin.datastream.server.dms.DatastreamResourceFactory;
import com.linkedin.datastream.server.dms.DatastreamResources;
import com.linkedin.datastream.server.dms.DatastreamStore;
import com.linkedin.datastream.server.dms.ZookeeperBackedDatastreamStore;

import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_ASSIGNMENT_STRATEGY_FACTORY;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_AUTHORIZER_NAME;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_BOOTSTRAP_TYPE;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_DEDUPER_FACTORY;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_NAMES;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CONNECTOR_PREFIX;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_CSV_METRICS_DIR;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_DIAG_PATH;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_DIAG_PORT;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_ENABLE_EMBEDDED_JETTY;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_FACTORY_CLASS_NAME;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_HTTP_PORT;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_SERDE_NAMES;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_SERDE_PREFIX;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_TRANSPORT_PROVIDER_NAMES;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.CONFIG_TRANSPORT_PROVIDER_PREFIX;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.DEFAULT_DEDUPER_FACTORY;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.DOMAIN_DEDUPER;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.DOMAIN_DIAG;
import static com.linkedin.datastream.server.DatastreamServerConfigurationConstants.STRATEGY_DOMAIN;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * DatastreamServer is the entry point for Brooklin. It is a container for all
 * datastream services including the REST API service, the coordinator and so on.
 */
public class DatastreamServer {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamServer.class);
  private static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
  private static final List<BrooklinMetricInfo> METRIC_INFOS = new ArrayList<>();

  private final String _csvMetricsDir;
  private final Map<String, String> _bootstrapConnectors;

  private Coordinator _coordinator;
  private DatastreamStore _datastreamStore;
  private DatastreamJettyStandaloneLauncher _jettyLauncher;
  private JmxReporter _jmxReporter;
  private ServerComponentHealthAggregator _serverComponentHealthAggregator;
  private int _httpPort;
  private boolean _isInitialized = false;
  private boolean _isStarted = false;

  static {
    // Instantiate a dynamic metrics manager singleton object so that other components can emit metrics on the fly
    DynamicMetricsManager.createInstance(METRIC_REGISTRY);
  }

  /**
   * Constructor for the DatastreamServer
   *
   * Sets up state and initializes various components, e.g.
   * <ul>
   *  <li>Initializes all connectors (including bootstrap connectors) declared in properties</li>
   *  <li>Initializes all transport providers declared in properties</li>
   *  <li>Initializes all SerDes declared in properties</li>
   *  <li>Sets up the coordinator with coordinator properties obtained from properties</li>
   *  <li>Sets up the jetty launcher</li>
   *  <li>Sets up the DMS endpoint server</li>
   *  <li>Initializes metrics</li>
   * </ul>
   * @param properties properties to set up the DatastreamServer with.
   * @throws DatastreamException if any of the following config properties is missing or empty:
   *  <ul>
   *    <li>{@value DatastreamServerConfigurationConstants#CONFIG_CONNECTOR_NAMES}</li>
   *    <li>{@value DatastreamServerConfigurationConstants#CONFIG_TRANSPORT_PROVIDER_NAMES}</li>
   *  </ul>
   */
  public DatastreamServer(Properties properties) throws DatastreamException {
    LOG.info("Start to initialize DatastreamServer. Properties: " + properties);
    LOG.info("Creating coordinator.");
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);

    HashSet<String> connectorTypes = new HashSet<>(verifiableProperties.getStringList(CONFIG_CONNECTOR_NAMES,
        Collections.emptyList()));
    if (connectorTypes.size() == 0) {
      String errorMessage = "No connectors specified in connectorTypes";
      LOG.error(errorMessage);
      throw new DatastreamRuntimeException(errorMessage);
    }

    HashSet<String> transportProviderNames =
        new HashSet<>(verifiableProperties.getStringList(CONFIG_TRANSPORT_PROVIDER_NAMES, Collections.emptyList()));
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
    LOG.info("Loading connectors {}", connectorTypes);
    _bootstrapConnectors = new HashMap<>();
    for (String connectorStr : connectorTypes) {
      initializeConnector(connectorStr,
          verifiableProperties.getDomainProperties(CONFIG_CONNECTOR_PREFIX + connectorStr),
          coordinatorConfig.getCluster());
    }

    LOG.info("Loading Transport providers {}", transportProviderNames);
    for (String tpName : transportProviderNames) {
      initializeTransportProvider(tpName,
          verifiableProperties.getDomainProperties(CONFIG_TRANSPORT_PROVIDER_PREFIX + tpName));
    }

    Set<String> serdeNames = new HashSet<>(verifiableProperties.getStringList(CONFIG_SERDE_NAMES, Collections.emptyList()));
    LOG.info("Loading Serdes {} ", serdeNames);
    for (String serde : serdeNames) {
      initializeSerde(serde, verifiableProperties.getDomainProperties(CONFIG_SERDE_PREFIX + serde));
    }

    _datastreamStore = new ZookeeperBackedDatastreamStore(datastreamCache, zkClient, coordinatorConfig.getCluster());

    boolean enableEmbeddedJetty = verifiableProperties.getBoolean(CONFIG_ENABLE_EMBEDDED_JETTY, true);

    // Port will be updated after start() is called if it is 0
    _httpPort = verifiableProperties.getInt(CONFIG_HTTP_PORT, 0);
    Validate.isTrue(_httpPort == 0 || _httpPort >= 1024, "Invalid port number: " + _httpPort);

    if (enableEmbeddedJetty) {
      _jettyLauncher = new DatastreamJettyStandaloneLauncher(_httpPort, new DatastreamResourceFactory(this),
          "com.linkedin.datastream.server.dms", "com.linkedin.datastream.server.diagnostics");
    }

    Properties diagProperties = verifiableProperties.getDomainProperties(DOMAIN_DIAG);
    String diagPortStr = diagProperties.getProperty(CONFIG_DIAG_PORT, "");
    int diagPort = diagPortStr.isEmpty() ? _httpPort : Integer.valueOf(diagPortStr);
    String diagPath = diagProperties.getProperty(CONFIG_DIAG_PATH, "");
    _serverComponentHealthAggregator = new ServerComponentHealthAggregator(zkClient, coordinatorConfig.getCluster(), diagPort, diagPath);

    _csvMetricsDir = verifiableProperties.getString(CONFIG_CSV_METRICS_DIR, "");

    verifiableProperties.verify();

    initializeMetrics();

    _isInitialized = true;

    LOG.info("DatastreamServer initialized successfully.");
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

  public ServerComponentHealthAggregator getServerComponentHealthAggregator() {
    return _serverComponentHealthAggregator;
  }

  private void initializeSerde(String serdeName, Properties serdeConfig) {
    LOG.info("Starting to load the serde:{} with config: {} ", serdeName, serdeConfig);

    String factoryClassName = serdeConfig.getProperty(CONFIG_FACTORY_CLASS_NAME, "");
    if (StringUtils.isBlank(factoryClassName)) {
      String msg = "Factory class name is not set or empty for serde: " + serdeName;
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    SerdeAdminFactory factory = ReflectionUtils.createInstance(factoryClassName);
    if (factory == null) {
      String msg = "Invalid class name or no parameter-less constructor, class=" + factoryClassName;
      LOG.error(msg);
      throw new DatastreamRuntimeException(msg);
    }

    SerdeAdmin admin = factory.createSerdeAdmin(serdeName, serdeConfig);
    _coordinator.addSerde(serdeName, admin);
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

  private void initializeConnector(String connectorName, Properties connectorProperties, String clusterName) {
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

    Connector connectorInstance =
        connectorFactoryInstance.createConnector(connectorName, connectorProperties, clusterName);

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

    // Read the deduper from the config; if not found, use default strategy
    DatastreamDeduperFactory deduperFactoryInstance = null;
    String deduperFactory = connectorProperties.getProperty(CONFIG_CONNECTOR_DEDUPER_FACTORY, DEFAULT_DEDUPER_FACTORY);
    if (!deduperFactory.isEmpty()) {
      deduperFactoryInstance = ReflectionUtils.createInstance(deduperFactory);
    }

    if (deduperFactoryInstance == null) {
      String errorMessage = "Invalid de-duper factory class: " + deduperFactory;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    Properties deduperProps = connectorProps.getDomainProperties(DOMAIN_DEDUPER);
    DatastreamDeduper deduper = deduperFactoryInstance.createDatastreamDeduper(deduperProps);

    boolean customCheckpointing =
        Boolean.parseBoolean(connectorProperties.getProperty(CONFIG_CONNECTOR_CUSTOM_CHECKPOINTING, "false"));

    String authorizerName = connectorProps.getString(CONFIG_CONNECTOR_AUTHORIZER_NAME, null);
    _coordinator.addConnector(connectorName, connectorInstance, assignmentStrategy, customCheckpointing,
        deduper, authorizerName);

    LOG.info("Connector loaded successfully. Type: " + connectorName);
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
        LOG.info("csvMetricsDir {} doesn't exist, creating it.", _csvMetricsDir);
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

  /**
   * Starts the DatastreamServer
   * This starts up the JMX reporter, coordinator, and the DMS REST endpoint.
   * @throws DatastreamException if starting the HTTP jetty server fails
   */
  public synchronized void startup() throws DatastreamException {
    // Start the JMX reporter
    if (_jmxReporter != null) {
      _jmxReporter.start();
    }

    // Start the coordinator
    if (_coordinator != null) {
      _coordinator.start();
    }

    // Start the DMS REST endpoint.
    try {
      _jettyLauncher.start();
      _httpPort = _jettyLauncher.getPort();
      // httpPort might be modified when _jettyLauncher start, so set the port of _serverComponentHealthAggregator.
      _serverComponentHealthAggregator.setPort(_httpPort);
      _isStarted = true;
    } catch (Exception ex) {
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, "Failed to start embedded Jetty.", ex);
    }
  }

  /**
   * Shuts down the DatastreamServer
   * This stops the JMX reporter, coordinator, and DMS REST endpoint.
   */
  public synchronized void shutdown() {
    if (_coordinator != null) {
      _coordinator.stop();
      _coordinator = null;
    }
    _datastreamStore = null;
    if (_jettyLauncher != null) {
      try {
        _jettyLauncher.stop();
      } catch (Exception e) {
        LOG.error("Failed to shutdown embedded Jetty.", e);
      }
      _jettyLauncher = null;
    }

    if (_jmxReporter != null) {
      _jmxReporter.stop();
    }
    _isInitialized = false;
    _isStarted = false;
  }

  /**
   * The main entry point for Brooklin server application
   *
   * Expects a Java properties configuration file containing the server
   * properties to use for setting up a {@link DatastreamServer} instance.
   */
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

  /**
   * Load properties from the specified Java properties file
   * @param filename  Properties file path
   */
  public static Properties loadProps(String filename) throws IOException {
    Properties props = new Properties();

    try (InputStream propStream = new FileInputStream(filename)) {
      props.load(propStream);
    }

    return props;
  }
}
