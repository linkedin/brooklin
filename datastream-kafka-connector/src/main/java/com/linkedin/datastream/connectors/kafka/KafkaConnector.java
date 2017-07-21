package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.PartitionInfo;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.ThreadUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


public class KafkaConnector implements Connector {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnector.class);
  private static final Duration RETRY_SLEEP_DURATION = Duration.ofSeconds(5);
  private static final int DEFAULT_RETRY_COUNT = 5;

  public static final String CONNECTOR_NAME = "kafka";
  public static final String DOMAIN_KAFKA_CONSUMER = "consumer";
  public static final String CONFIG_COMMIT_INTERVAL_MILLIS = "commitIntervalMs";
  public static final String CONFIG_CONSUMER_FACTORY_CLASS = "consumerFactoryClassName";
  public static final String CONFIG_WHITE_LISTED_CLUSTERS = "whiteListedClusters";
  public static final String CONFIG_DEFAULT_KEY_SERDE = "defaultKeySerde";
  public static final String CONFIG_DEFAULT_VALUE_SERDE = "defaultValueSerde";
  public static final String CONFIG_RETRY_COUNT = "retryCount";
  private final String _defaultKeySerde;
  private final String _defaultValueSerde;
  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final Properties _connectorProprs;
  private final Properties _consumerProps;
  private final Set<KafkaBrokerAddress> _whiteListedBrokers;

  public KafkaConnector(String name, Properties config) {
    _name = name;
    _connectorProprs = config;
    VerifiableProperties verifiableProperties = new VerifiableProperties(config);
    _defaultKeySerde = verifiableProperties.getString(CONFIG_DEFAULT_KEY_SERDE, "");
    _defaultValueSerde = verifiableProperties.getString(CONFIG_DEFAULT_VALUE_SERDE, "");
    _commitIntervalMillis = verifiableProperties.getLongInRange(KafkaConnector.CONFIG_COMMIT_INTERVAL_MILLIS,
        Duration.ofMinutes(1).toMillis(), 0, Long.MAX_VALUE);
    _retryCount = verifiableProperties.getInt(CONFIG_RETRY_COUNT, DEFAULT_RETRY_COUNT);

    String factory = verifiableProperties.getString(KafkaConnector.CONFIG_CONSUMER_FACTORY_CLASS,
        KafkaConsumerFactoryImpl.class.getName());
    _consumerFactory = ReflectionUtils.createInstance(factory);
    if (_consumerFactory == null) {
      throw new DatastreamRuntimeException("Unable to instantiate factory class: " + factory);
    }

    List<KafkaBrokerAddress> brokers =
        Optional.ofNullable(verifiableProperties.getString(KafkaConnector.CONFIG_WHITE_LISTED_CLUSTERS, null))
            .map(KafkaConnectionString::parseBrokers)
            .orElse(Collections.emptyList());
    _whiteListedBrokers = new HashSet<>(brokers);

    _consumerProps = verifiableProperties.getDomainProperties(DOMAIN_KAFKA_CONSUMER);
  }

  private final String _name;
  private final long _commitIntervalMillis;
  private final int _retryCount;
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
    }

    for (DatastreamTask task : tasks) {
      if (_runningTasks.containsKey(task)) {
        continue; //already running
      }
      LOG.info("creating task for {}.", task);
      KafkaConnectorTask connectorTask =
          new KafkaConnectorTask(_consumerFactory, _consumerProps, task, _commitIntervalMillis, RETRY_SLEEP_DURATION,
              _retryCount);
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

      if (stream.hasDestination()) {
        // set default key and value serde
        if (!stream.getDestination().hasKeySerDe() && !StringUtils.isBlank(_defaultKeySerde)) {
          stream.getDestination().setKeySerDe(_defaultKeySerde);
        }
        if (!stream.getDestination().hasPayloadSerDe() && !StringUtils.isBlank(_defaultValueSerde)) {
          stream.getDestination().setPayloadSerDe(_defaultValueSerde);
        }
      }

      if (!isWhiteListedCluster(parsed)) {
        String msg =
            String.format("Kafka connector is not white-listed for the cluster %s. Current white-listed clusters %s.",
                connectionString, _whiteListedBrokers);
        LOG.error(msg);
        throw new DatastreamValidationException(msg);
      }
      try (Consumer<?, ?> consumer = KafkaConnectorTask.createConsumer(_consumerFactory, _consumerProps,
          "partitionFinder", parsed)) {
        List<PartitionInfo> partitionInfos = consumer.partitionsFor(parsed.getTopicName());
        if (partitionInfos == null) {
          throw new DatastreamValidationException(
              "Can't get partition info from kafka. Very likely that auto topic creation "
                  + "is disabled on the broker and the topic doesn't exist: " + parsed.getTopicName());
        }
        int numPartitions = partitionInfos.size();
        if (!source.hasPartitions()) {
          LOG.info("Kafka source {} has {} partitions.", parsed, numPartitions);
          source.setPartitions(numPartitions);
        } else {
          if (source.getPartitions() != numPartitions) {
            String msg =
                String.format("Source is configured with %d partitions, But the topic %s actually has %d partitions",
                    source.getPartitions(), parsed.getTopicName(), numPartitions);
            LOG.error(msg);
            throw new DatastreamValidationException(msg);
          }
        }

        // Try to see if the start positions requested are valid.
        // Value is a json string encoding partition to offset, like {"0":23,"1":15,"2":88}
        if (stream.getMetadata().containsKey(DatastreamMetadataConstants.START_POSITION)) {
          String json = stream.getMetadata().get(DatastreamMetadataConstants.START_POSITION);
          Map<Integer, Long> offsetMap = JsonUtils.fromJson(json, new TypeReference<Map<Integer, Long>>() {
          });
          if (offsetMap.size() != numPartitions || IntStream.range(0, numPartitions)
              .anyMatch(x -> !offsetMap.containsKey(x))) {
            String msg =
                String.format("Missing partitions starting offset for datastream %s, json value %s", stream, json);
            LOG.warn(msg);
            throw new DatastreamValidationException(msg);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Initialization threw an exception.", e);
      throw new DatastreamValidationException(e);
    }
  }

  private Boolean isWhiteListedCluster(KafkaConnectionString connectionStr) {
    return _whiteListedBrokers.isEmpty() || connectionStr.getBrokers().stream().anyMatch(_whiteListedBrokers::contains);
  }
}
