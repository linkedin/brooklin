package com.linkedin.datastream.connectors.oracle.triggerbased;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.SchemaRegistryClient;
import com.linkedin.datastream.common.SchemaRegistryClientFactory;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OracleConnector implements Connector {

  private static final Logger LOG = LoggerFactory.getLogger(OracleConnector.class);

  static final String CONNECTOR_TYPE = "OracleTriggerBased";
  private static final String SCHEMA_REGISTRY_FACTORY_KEY = "schemaRegistryFactory";

  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);
  private ScheduledExecutorService _daemonThreadExecutorService = null;
  private final OracleConnectorConfig _config;
  private final SchemaRegistryClient _schemaRegistry;

  private ExecutorService _executorService = Executors.newCachedThreadPool(new ThreadFactory() {
    private AtomicInteger threadCounter = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(false);
      t.setPriority(Thread.NORM_PRIORITY);
      t.setName(CONNECTOR_TYPE + "-worker-thread-" + threadCounter.incrementAndGet());
      return t;
    }
  });

  /**
   * This Oracle Connector class is the main implementation of the Connector
   * interface for the Oracle TriggerBased Connector
   *
   * The Connector Class implements the onAssignmentChange function which
   * gets run every time there is a new DatastreamTask that got assigned
   * to this instance of the Connector. At that point, the Connector will
   * instantiate a new OracleTaskHandler.
   *
   * Each task Handler has an OracleConsumer and a MessageHandler. The OracleConsumer
   * is responsible for query changed rows form the Datastream source and The
   * Message Handler is responsible pushing data into the Transport Provider.
   *
   * There is one OracleTaskHandler per DatastreamTask. This is so that tasks with the same
   * source but different destinations never interfere with each other.
   * It also means that datastreams with the same source but at different times
   * are independent (imagine deleting a datastreamTask and then
   * re-created with the same source)
   *
   ┌──────────────────────┐
   │                      │                                ┌───────────────────┐
   │ ┌──────────────────┐ │                                │OracleTaskHandler 1│
   │ │ DatastreamTask 1 │ │                             ┌─▶│                   │
   │ │                  │ │    ┌─────────────────────┐  │  └───────────────────┘
   │ └──────────────────┘ │    │                     │  │
   │                      │    │                     │  │  ┌───────────────────┐
   │ ┌──────────────────┐ │    │  OracleTBConnector  │  │  │OracleTaskHandler 2│
   │ │ DatastreamTask 2 │ │───▶│                     │──┼─▶│                   │
   │ │                  │ │    │                     │  │  └───────────────────┘
   │ └──────────────────┘ │    │                     │  │
   │                      │    └─────────────────────┘  │  ┌───────────────────┐
   │ ┌──────────────────┐ │                             │  │OracleTaskHandler 3│
   │ │ DatastreamTask 3 │ │                             └─▶│                   │
   │ │                  │ │                                └───────────────────┘
   │ └──────────────────┘ │
   └──────────────────────┘
   *
   */
  public OracleConnector(Properties properties) throws DatastreamException {
    _config = new OracleConnectorConfig(properties);
    _schemaRegistry = createSchemaRegistryClient(properties, _config);
    // TODO implement
  }

  public void start() {
    LOG.info("Oracle TriggerBased Connector start Requested");
    // TODO implement
  }

  public void stop() {
    LOG.info("Oracle TriggerBased connector stop requested.");
    // TODO implement
  }

  public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
    if (tasks == null) {
      LOG.error("onAssignmentChange with null tasks");
      return;
    }

    LOG.info("onAssignmentChange called with {}", tasks);
    // TODO implement
  }

  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams) throws DatastreamValidationException {
    LOG.info("initializeDatastream called for %s", stream);
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    // metrics.addAll(OracleTaskHandler.getMetricInfos());
    return Collections.unmodifiableList(metrics);
  }

  /**
   * Initialize a SchemaRegistryClient implementation instance.
   *
   * This function first uses ReflectionUtils to create a
   * schemaRegistryFactory instance based on Properties. We then
   * instantiate certain schemaRegistry based on Connector
   * Properties
   *
   * @param properties - SchemaRegistryFactory className
   * @param config - SchemaRegistry configs such as `mode` and `uri`
   * @return SchemaRegistryClient
   */
  private SchemaRegistryClient createSchemaRegistryClient(Properties properties, OracleConnectorConfig config) {
    // instantiate a schemaRegistryFactory implementation instance based on the
    // class name given in the configs
    String className = properties.getProperty(SCHEMA_REGISTRY_FACTORY_KEY);
    if (StringUtils.isBlank(className)) {
      String errorMessage = "Factory className is empty for SchemaRegistryClientFactory";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    SchemaRegistryClientFactory schemaRegistryFactory = ReflectionUtils.createInstance(className);
    if (schemaRegistryFactory == null) {
      String msg = "Invalid class name or no parameter-less constructor, class=" + className;
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, msg, null);
    }

    // use factory to instantiate SchemaRegistry
    return schemaRegistryFactory.createSchemaRegistryClient(config.getLiKafkaSchemaRegistryConfig());
  }
}
