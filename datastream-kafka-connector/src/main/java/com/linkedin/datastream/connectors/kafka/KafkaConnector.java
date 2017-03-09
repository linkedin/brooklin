package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.ThreadUtils;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


public class KafkaConnector implements Connector {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnector.class);

  public static final String CONNECTOR_NAME = "kafka";
  public static final String CONFIG_COMMIT_INTERVAL_MILLIS = "commitIntervalMillis";
  public static final String CONFIG_CONSUMER_FACTORY_CLASS = "consumerFactoryClassName";
  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final Properties _consumerProps;

  public KafkaConnector(String name, long commitIntervalMillis, KafkaConsumerFactory<?, ?> kafkaConsumerFactory,
      Properties kafkaConsumerProps) {
    _consumerFactory = kafkaConsumerFactory;
    _consumerProps = kafkaConsumerProps;
    _name = name;
    _commitIntervalMillis = commitIntervalMillis;
  }

  private final String _name;
  private final long _commitIntervalMillis;
  private final ExecutorService _executor = Executors.newCachedThreadPool(new ThreadFactory() {
    private AtomicInteger threadCounter = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setDaemon(true);
      t.setName(_name + " worker thread " + threadCounter.incrementAndGet());
      t.setUncaughtExceptionHandler((thread, e) -> {
        LOG.error("thread " + thread.getName() + " has died due to uncaught exception", e);
      });
      return t;
    }
  });
  private final ConcurrentHashMap<DatastreamTask, KafkaConnectorTask> _runningTasks = new ConcurrentHashMap<>();

  @Override
  public void start() {
    //nop
  }

  @Override
  public void stop() {
    _runningTasks.values().forEach(KafkaConnectorTask::stop);
    //TODO - should wait?
    _runningTasks.clear();
    if (!ThreadUtils.shutdownExecutor(_executor, Duration.of(5, ChronoUnit.SECONDS), LOG)) {
      LOG.warn("Failed to shut down cleanly.");
    }
    LOG.info("stopped.");
  }

  @Override
  public void onAssignmentChange(List<DatastreamTask> tasks) {
    LOG.info("onAssignmentChange called with tasks {}", tasks);

    HashSet<DatastreamTask> toCancel = new HashSet<>(_runningTasks.keySet());
    toCancel.removeAll(tasks);

    for (DatastreamTask task : toCancel) {
      KafkaConnectorTask connectorTask = _runningTasks.remove(task);
      connectorTask.stop();
      //TODO - should wait?
    }

    for (DatastreamTask task : tasks) {
      if (_runningTasks.containsKey(task)) {
        continue; //already running
      }
      LOG.info("creating task for {}.", task);
      KafkaConnectorTask connectorTask = new KafkaConnectorTask(_consumerFactory, _consumerProps, task, 100);
      _runningTasks.put(task, connectorTask);
      _executor.submit(connectorTask);
    }
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return Collections.unmodifiableList(KafkaConnectorTask.getMetricInfos());
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    LOG.info("Initialize datastream {}", stream);
    DatastreamSource source = stream.getSource();
    String connectionString = source.getConnectionString();
    //TODO - better validation and canonicalization
    //its possible to list the same broker as a hostname or IP
    //(kafka://localhost:666 vs kafka://127.0.0.1:666 vs kafka://::1:666/topic)
    //the "best" thing to do would be connect to _ALL_ brokers listed, and from each broker
    //get the cluster members and the cluster unique ID (which only exists in kafka ~0.10+)
    //and then:
    //1. fail if brokers listed are member of different clusters
    //2. "normalize" the connection string to be either all members as they appear in metadata
    //   or have the cluster unique ID somehow
    try {
      KafkaConnectionString parsed = KafkaConnectionString.valueOf(connectionString);
      source.setConnectionString(parsed.toString()); //ordered now
    } catch (IllegalArgumentException e) {
      throw new DatastreamValidationException(e);
    }
  }
}
