package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.ThreadUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;


/**
 * Base class for connectors that consume from Kafka.
 */
public abstract class AbstractKafkaConnector implements Connector {

  protected static final Duration RETRY_SLEEP_DURATION = Duration.ofSeconds(5);
  protected static final int DEFAULT_RETRY_COUNT = 5;

  public static final String DOMAIN_KAFKA_CONSUMER = "consumer";
  public static final String CONFIG_COMMIT_INTERVAL_MILLIS = "commitIntervalMs";
  public static final String CONFIG_CONSUMER_FACTORY_CLASS = "consumerFactoryClassName";
  public static final String CONFIG_DEFAULT_KEY_SERDE = "defaultKeySerde";
  public static final String CONFIG_DEFAULT_VALUE_SERDE = "defaultValueSerde";
  public static final String CONFIG_RETRY_COUNT = "retryCount";
  protected final String _defaultKeySerde;
  protected final String _defaultValueSerde;
  protected final KafkaConsumerFactory<?, ?> _consumerFactory;
  protected final Properties _consumerProps;

  protected final String _name;
  protected final long _commitIntervalMillis;
  protected final int _retryCount;

  private final Logger _logger;
  protected final ConcurrentHashMap<DatastreamTask, AbstractKafkaBasedConnectorTask> _runningTasks =
      new ConcurrentHashMap<>();
  protected final ExecutorService _executor = Executors.newCachedThreadPool(new ThreadFactory() {
    private AtomicInteger threadCounter = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName(_name + " worker thread " + threadCounter.incrementAndGet());
      t.setUncaughtExceptionHandler((thread, e) -> {
        _logger.error("thread " + thread.getName() + " has died due to uncaught exception", e);
      });
      return t;
    }
  });

  public AbstractKafkaConnector(String connectorName, Properties config, Logger logger) {
    _name = connectorName;
    _logger = logger;
    VerifiableProperties verifiableProperties = new VerifiableProperties(config);
    _defaultKeySerde = verifiableProperties.getString(CONFIG_DEFAULT_KEY_SERDE, "");
    _defaultValueSerde = verifiableProperties.getString(CONFIG_DEFAULT_VALUE_SERDE, "");
    _commitIntervalMillis = verifiableProperties.getLongInRange(CONFIG_COMMIT_INTERVAL_MILLIS,
        Duration.ofMinutes(1).toMillis(), 0, Long.MAX_VALUE);
    _retryCount = verifiableProperties.getInt(CONFIG_RETRY_COUNT, DEFAULT_RETRY_COUNT);

    String factory = verifiableProperties.getString(CONFIG_CONSUMER_FACTORY_CLASS,
        KafkaConsumerFactoryImpl.class.getName());
    _consumerFactory = ReflectionUtils.createInstance(factory);
    if (_consumerFactory == null) {
      throw new DatastreamRuntimeException("Unable to instantiate factory class: " + factory);
    }

    _consumerProps = verifiableProperties.getDomainProperties(DOMAIN_KAFKA_CONSUMER);
  }

  protected abstract AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task);

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {
    _logger.info("onAssignmentChange called with tasks {}", tasks);

    HashSet<DatastreamTask> toCancel = new HashSet<>(_runningTasks.keySet());
    toCancel.removeAll(tasks);

    for (DatastreamTask task : toCancel) {
      AbstractKafkaBasedConnectorTask connectorTask = _runningTasks.remove(task);
      connectorTask.stop();
    }

    for (DatastreamTask task : tasks) {
      AbstractKafkaBasedConnectorTask kafkaBasedConnectorTask = _runningTasks.get(task);
      if (kafkaBasedConnectorTask != null) {
        kafkaBasedConnectorTask.checkAndUpdateTask(task);
        // make sure to replace the DatastreamTask with most up to date info
        _runningTasks.remove(task);
        _runningTasks.put(task, kafkaBasedConnectorTask);
        continue; // already running
      }
      _logger.info("creating task for {}.", task);
      AbstractKafkaBasedConnectorTask connectorTask = createKafkaBasedConnectorTask(task);
      _runningTasks.put(task, connectorTask);
      _executor.submit(connectorTask);
    }
  }

  @Override
  public void start() {
    //nop
  }

  @Override
  public void stop() {
    _runningTasks.values().forEach(AbstractKafkaBasedConnectorTask::stop);
    //TODO - should wait?
    _runningTasks.clear();
    if (!ThreadUtils.shutdownExecutor(_executor, Duration.of(5, ChronoUnit.SECONDS), _logger)) {
      _logger.warn("Failed to shut down cleanly.");
    }
    _logger.info("stopped.");
  }

}
