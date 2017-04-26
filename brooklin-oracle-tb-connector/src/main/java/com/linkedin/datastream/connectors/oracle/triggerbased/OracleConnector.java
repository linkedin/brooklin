package com.linkedin.datastream.connectors.oracle.triggerbased;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.SchemaRegistryClient;
import com.linkedin.datastream.common.SchemaRegistryClientFactory;
import com.linkedin.datastream.common.ThreadUtils;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.connectors.oracle.triggerbased.consumer.OracleConsumerConfig;


public class OracleConnector implements Connector {

  private static final Logger LOG = LoggerFactory.getLogger(OracleConnector.class);

  private static final String CONNECTOR_TYPE = "OracleTriggerBased";
  private static final String SCHEMA_REGISTRY_FACTORY_KEY = "schemaRegistryFactory";
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(30);

  private ScheduledExecutorService _daemonThreadExecutorService = null;
  private final OracleConnectorConfig _config;
  private final SchemaRegistryClient _schemaRegistry;

  // mapping between one OracleTaskHandler per DatastreamTask
  private Map<DatastreamTask, OracleTaskHandler> _namedTaskHandlers;


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
    _namedTaskHandlers = new HashMap<>();
  }

  /**
   * The Oracle Connector maintains a pool of OracleTaskHandlers. Each handler
   * associated with one specific datastreamTask. Each handler also runs in
   * its own thread for concurrency.
   *
   * There is also a daemon thread that gets created when calling
   * the {@code ::start()} method, which constantly checks to make sure that
   * the handlers are operating.
   */
  public void start() {
    LOG.info("Oracle TriggerBased Connector start Requested");

    if (_daemonThreadExecutorService == null) {
      _daemonThreadExecutorService = Executors.newSingleThreadScheduledExecutor();
      _daemonThreadExecutorService.scheduleAtFixedRate(() -> {
        if (_namedTaskHandlers != null && !_namedTaskHandlers.isEmpty()) {
          synchronized (this) {
            _namedTaskHandlers.values().forEach(OracleTaskHandler::restartIfNotRunning);
          }
        } else {
          LOG.warn("Oracle TriggerBased connector has not received any DatastreamTasks yet");
        }
      }, _config.getDaemonThreadIntervalSeconds(), _config.getDaemonThreadIntervalSeconds(), TimeUnit.SECONDS);
      LOG.info(String.format("Scheduled for daemon thread to run every %d seconds", _config.getDaemonThreadIntervalSeconds()));
    }

    LOG.info("Oracle connector started.");
  }

  /**
   * Shutdown each {@link OracleTaskHandler} as well as stop the daemon thread
   */
  public void stop() {
    LOG.info(String.format("attempting to shutdown %s connector ...", CONNECTOR_TYPE));
    _namedTaskHandlers.values().forEach(OracleTaskHandler::stop);

    if (!ThreadUtils.shutdownExecutor(_executorService, SHUTDOWN_TIMEOUT, LOG)) {
      LOG.warn("Failed to shut down OracleTaskHandler Executor Service cleanly");
    }

    if (!ThreadUtils.shutdownExecutor(_daemonThreadExecutorService, SHUTDOWN_TIMEOUT, LOG)) {
      LOG.warn("Failed to shut down daemon Thread Executor Service cleanly");
    }

    _daemonThreadExecutorService = null;
    _namedTaskHandlers.clear();

    LOG.info(String.format("%s connector has successfully stopped", CONNECTOR_TYPE));
  }

  public synchronized void onAssignmentChange(List<DatastreamTask> tasks) {
    if (tasks == null) {
      LOG.error("onAssignmentChange with null tasks");
      return;
    }

    LOG.info("onAssignmentChange called with " + tasks);
    Set<DatastreamTask> tasksToStop = new HashSet<>(_namedTaskHandlers.keySet());

    tasks.forEach(task -> {

      LOG.info(String.format("Handling task: %s with source: %s",
          task.getDatastreamTaskName(),
          task.getDatastreamSource().getConnectionString()));

      // pardon this task from the remove list
      tasksToStop.remove(task);

      // This is a brand new datastreamTask
      if (!_namedTaskHandlers.containsKey(task)) {
        OracleSource oracleSource = new OracleSource(task.getDatastreamSource().getConnectionString());

        OracleConsumerConfig consumerConfig = _config.getOracleConsumerConfig(oracleSource.getDbName());
        OracleTaskHandler taskHandler = new OracleTaskHandler(oracleSource, _executorService, _schemaRegistry, task);
        _namedTaskHandlers.put(task, taskHandler);

        try {
          taskHandler.initializeConsumer(consumerConfig);
          taskHandler.start();

        } catch (DatastreamException e) {
          LOG.error(String.format("Failed to create Task Handler for Task: %s", task.getDatastreamTaskName()));
        }
      }
    });

    LOG.info("Tasks to stop: {}", tasksToStop);

    tasksToStop.forEach(task -> {
      _namedTaskHandlers.get(task).stop();
      _namedTaskHandlers.remove(task);
    });
  }

  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    LOG.info("initializeDatastream called for {}", stream);
    // due to the fact there is no partitioning in Oracle
    stream.getSource().setPartitions(1);

    String schemaId = null;
    try {
      schemaId = SchemaUtil.getSchemaId(stream);
    } catch (NullPointerException e) {
      throw new DatastreamValidationException(
          "No " + SchemaUtil.SCHEMA_ID_KEY + " in metadata of stream: " + stream.getName());
    }

    try {
      SchemaUtil.getSchemaById(schemaId, _schemaRegistry);
    } catch (DatastreamException e) {
      throw new DatastreamValidationException(e);
    }
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(OracleTaskHandler.getMetricInfos());
    return Collections.unmodifiableList(metrics);
  }

  /**
   * Initialize a SchemaRegistryClient implementation instance.
   *
   * This method first uses ReflectionUtils to create a
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
    return schemaRegistryFactory.createSchemaRegistryClient(config.getSchemaRegistryConfig());
  }
}
