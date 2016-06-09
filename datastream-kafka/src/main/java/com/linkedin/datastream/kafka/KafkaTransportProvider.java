package com.linkedin.datastream.kafka;

import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Metric;
import kafka.admin.AdminUtils;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DynamicMetricsManager;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.Pair;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


/**
 * This is Kafka Transport provider that writes events to kafka.
 */
public class KafkaTransportProvider implements TransportProvider {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaTransportProvider.class);

  private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private static final String VAL_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private static final String TOPIC_RETENTION_MS = "retention.ms";

  public static final String CONFIG_ZK_CONNECT = "zookeeper.connect";
  public static final String CONFIG_RETENTION_MS = "retentionMs";

  public static final String DEFAULT_REPLICATION_FACTOR = "1";
  public static final Duration DEFAULT_RETENTION = Duration.ofDays(3);

  private final KafkaProducer<byte[], byte[]> _producer;
  private final String _brokers;
  private final String _zkAddress;
  private static final String DESTINATION_URI_FORMAT = "kafka://%s/%s";
  private final ZkClient _zkClient;
  private final ZkUtils _zkUtils;
  private final Duration _retention;

  private final DynamicMetricsManager _dynamicMetricsManager;
  private static final String NUM_EVENTS_METRIC_NAME = "numDataEvents";
  private final Meter _eventWriteRate;
  private final Meter _eventByteWriteRate;
  private final Meter _eventTransportErrorRate;

  public KafkaTransportProvider(Properties props) {
    LOG.info(String.format("Creating kafka transport provider with properties: %s", props));
    if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      String errorMessage = "Bootstrap servers are not set";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    if (!props.containsKey(CONFIG_ZK_CONNECT)) {
      String errorMessage = "Zk connection string config is not set";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    _brokers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    _zkAddress = props.getProperty(CONFIG_ZK_CONNECT);
    _zkClient = new ZkClient(_zkAddress);
    ZkConnection zkConnection = new ZkConnection(_zkAddress);
    _zkUtils = new ZkUtils(_zkClient, zkConnection, false);

    if (props.containsKey(CONFIG_RETENTION_MS)) {
      _retention = Duration.ofMillis(Long.parseLong(props.getProperty(CONFIG_RETENTION_MS)));
    } else {
      _retention = DEFAULT_RETENTION;
    }

    // Assign mandatory arguments
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VAL_SERIALIZER);

    _producer = new KafkaProducer<>(props);

    // initialize metrics
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _eventWriteRate = new Meter();
    _eventByteWriteRate = new Meter();
    _eventTransportErrorRate = new Meter();
  }

  private ProducerRecord<byte[], byte[]> convertToProducerRecord(KafkaDestination destination, DatastreamProducerRecord record,
      byte[] key, byte[] payload)
      throws DatastreamException {

    Optional<Integer> partition = record.getPartition();

    if (partition.isPresent() && partition.get() >= 0) {
      return new ProducerRecord<>(destination.topicName(), partition.get(), key, payload);
    } else {
      return new ProducerRecord<>(destination.topicName(), key, payload);
    }
  }

  @Override
  public String createTopic(String topicName, int numberOfPartitions, Properties topicConfig) {
    Validate.notNull(topicName, "topicName should not be null");
    Validate.notNull(topicConfig, "topicConfig should not be null");

    int replicationFactor = Integer.parseInt(topicConfig.getProperty("replicationFactor", DEFAULT_REPLICATION_FACTOR));
    LOG.info(String.format("Creating topic with name %s partitions=%d with properties %s", topicName,
        numberOfPartitions, topicConfig));

    // Add default retention if no topic-level retention is specified
    if (!topicConfig.containsKey(TOPIC_RETENTION_MS)) {
      topicConfig.put(TOPIC_RETENTION_MS, String.valueOf(_retention.toMillis()));
    }

    try {
      // Create only if it doesn't exist.
      if (!AdminUtils.topicExists(_zkUtils, topicName)) {
        AdminUtils.createTopic(_zkUtils, topicName, numberOfPartitions, replicationFactor, topicConfig);
      } else {
        LOG.warn(String.format("Topic with name %s already exists", topicName));
      }
    } catch (Throwable e) {
      LOG.error(String.format("Creating topic %s failed with exception %s ", topicName, e));
      throw e;
    }

    return String.format(DESTINATION_URI_FORMAT, _zkAddress, topicName);
  }

  private String getTopicName(String destination) {
    return URI.create(destination).getPath().substring(1);
  }

  @Override
  public void dropTopic(String destinationUri) {
    Validate.notNull(destinationUri, "destinationuri should not null");
    String topicName = getTopicName(destinationUri);

    try {
      // Delete only if it exist.
      if (AdminUtils.topicExists(_zkUtils, topicName)) {
        AdminUtils.deleteTopic(_zkUtils, topicName);
      } else {
        LOG.warn(String.format("Trying to delete topic %s that doesn't exist", topicName));
      }
    } catch (Throwable e) {
      LOG.error(String.format("Deleting topic %s failed with exception %s", topicName, e));
      throw e;
    }
  }

  @Override
  public void send(String destinationUri, DatastreamProducerRecord record, SendCallback onSendComplete) {
    try {
      Validate.notNull(record, "null event record.");
      Validate.notNull(record.getEvents(), "null datastream events.");


      LOG.debug("Sending Datastream event record: " + record);

      for (Pair<byte[], byte[]> event : record.getEvents()) {
        ProducerRecord<byte[], byte[]> outgoing;
        try {
          KafkaDestination destination = KafkaDestination.parseKafkaDestinationUri(destinationUri);
          _dynamicMetricsManager.createOrUpdateCounter(this.getClass(), destination.topicName(), NUM_EVENTS_METRIC_NAME, 1);
          outgoing = convertToProducerRecord(destination, record, event.getKey(), event.getValue());
        } catch (Exception e) {
          String errorMessage = String.format("Failed to convert DatastreamEvent (%s) to ProducerRecord.", event);
          LOG.error(errorMessage, e);
          throw new DatastreamRuntimeException(errorMessage, e);
        }
        _eventWriteRate.mark();
        _eventByteWriteRate.mark(event.getKey().length + event.getValue().length);

        _producer.send(outgoing, (metadata, exception) -> onSendComplete.onCompletion(
                new DatastreamRecordMetadata(String.valueOf(metadata.offset()), metadata.topic(), metadata.partition()),
                exception));
      }
    } catch (Exception e) {
      _eventTransportErrorRate.mark();
      String errorMessage = String.format("Sending event (%s) to topic %s and Kafka cluster (Metadata brokers) %s "
          + "failed with exception", record.getEvents(), destinationUri, _brokers);
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }

    LOG.debug("Done sending Datastream event record: " + record);
  }

  @Override
  public void close() {
    // Close the kafka producer connection immediately (timeout of zero). If timeout > 0 producer connection won't
    // close immediately and will result in out of order events. More details in the below slide.
    // http://www.slideshare.net/JiangjieQin/no-data-loss-pipeline-with-apache-kafka-49753844/10
    if (_producer != null) {
      _producer.close(0, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void flush() {
    _producer.flush();
  }

  /**
   * Consult Kafka to get the retention for a topic. This is not cached
   * in case the retention might be changed externally after creation.
   * If no topic-level retention is configured, this method returns null.
   *
   * @param destination Destination URI
   * @return topic retention or null if no such config
   */
  @Override
  public Duration getRetention(String destination) {
    Validate.notNull(destination, "null destination URI");
    String topicName = getTopicName(destination);
    Properties props = AdminUtils.fetchEntityConfig(_zkUtils, ConfigType.Topic(), topicName);
    if (!props.containsKey(TOPIC_RETENTION_MS)) {
      return null;
    }
    return Duration.ofMillis(Long.parseLong(props.getProperty(TOPIC_RETENTION_MS)));
  }

  @Override
  public Map<String, Metric> getMetrics() {
    Map<String, Metric> metrics = new HashMap<>();

    metrics.put(buildMetricName("eventsWrittenPerSec"), _eventWriteRate);
    metrics.put(buildMetricName("eventBytesWrittenPerSec"), _eventByteWriteRate);
    metrics.put(buildMetricName("eventTransportErrorCount"), _eventTransportErrorRate);

    /*
     * For dynamic metrics captured by regular expression, put an object corresponding to the type of metric that will be
     * created dynamically.
     *
     * For example, adding the following metric name:
     *
     * metrics.put(getDynamicMetricPrefixRegex() + "numEvents", new Counter());
     * will capture a counter metric with name matching "com.linkedin.datastream.kafka.KafkaTransportProvider.xxx.numEvents",
     * where xxx is the topic name.
     */
    metrics.put(getDynamicMetricPrefixRegex() + "numEvents", new Counter());

    return Collections.unmodifiableMap(metrics);
  }
}
