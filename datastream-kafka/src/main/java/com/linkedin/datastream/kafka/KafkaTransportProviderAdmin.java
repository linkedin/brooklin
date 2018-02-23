package com.linkedin.datastream.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.commons.lang.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.admin.AdminUtils;
import kafka.admin.RackAwareMode;
import kafka.server.ConfigType;
import kafka.utils.ZkUtils;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


public class KafkaTransportProviderAdmin implements TransportProviderAdmin {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaTransportProviderAdmin.class);
  private static final String TOPIC_RETENTION_MS = "retention.ms";

  public static final String CONFIG_RETENTION_MS = "retentionMs";
  public static final String DEFAULT_REPLICATION_FACTOR = "1";
  public static final Duration DEFAULT_RETENTION = Duration.ofDays(3);
  public static final String CONFIG_ZK_CONNECT = "zookeeper.connect";

  public static final String CONFIG_METRICS_NAMES_PREFIX = "metricsNamesPrefix";
  private final String _transportProviderMetricsNamesPrefix;

  private static final int DEFAULT_NUMBER_PARTITIONS = 1;
  private final String _zkAddress;
  private static final String DESTINATION_URI_FORMAT = "kafka://%s/%s";

  private final ZkClient _zkClient;
  private KafkaTransportProvider _kafkaTransportProvider;

  private final ZkUtils _zkUtils;
  private final Duration _retention;

  public KafkaTransportProviderAdmin(Properties transportProviderProperties) {

    if (!transportProviderProperties.containsKey(CONFIG_ZK_CONNECT)) {
      String errorMessage = "Zk connection string config is not set";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    _zkAddress = transportProviderProperties.getProperty(CONFIG_ZK_CONNECT);

    _zkClient = new ZkClient(_zkAddress);

    ZkConnection zkConnection = new ZkConnection(_zkAddress);

    _zkUtils = new ZkUtils(_zkClient, zkConnection, false);

    if (transportProviderProperties.containsKey(CONFIG_RETENTION_MS)) {
      _retention = Duration.ofMillis(Long.parseLong(transportProviderProperties.getProperty(CONFIG_RETENTION_MS)));
    } else {
      _retention = DEFAULT_RETENTION;
    }

    String metricsPrefix = transportProviderProperties.getProperty(CONFIG_METRICS_NAMES_PREFIX, null);
    if (metricsPrefix != null && !metricsPrefix.endsWith(".")) {
      _transportProviderMetricsNamesPrefix = metricsPrefix + ".";
    } else {
      _transportProviderMetricsNamesPrefix = metricsPrefix;
    }

    _kafkaTransportProvider =
        new KafkaTransportProvider(transportProviderProperties, _transportProviderMetricsNamesPrefix);
  }

  @Override
  public TransportProvider assignTransportProvider(DatastreamTask task) {
    Validate.notNull(task, "null task");
    return _kafkaTransportProvider;
  }

  @Override
  public void unassignTransportProvider(DatastreamTask task) {
    Validate.notNull(task, "null task");
    // Do nothing.
  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream, String destinationName) {
    if (!datastream.hasDestination()) {
      datastream.setDestination(new DatastreamDestination());
    }

    DatastreamDestination destination = datastream.getDestination();
    DatastreamSource source = datastream.getSource();

    if (!destination.hasConnectionString() || destination.getConnectionString().isEmpty()) {
      destination.setConnectionString(getDestination(destinationName));
    }

    if (!destination.hasPartitions() || destination.getPartitions() <= 0) {
      if (source.hasPartitions()) {
        destination.setPartitions(source.getPartitions());
      } else {
        destination.setPartitions(DEFAULT_NUMBER_PARTITIONS);
      }
    }
  }

  public String getDestination(String topicName) {
    return String.format(DESTINATION_URI_FORMAT, _zkAddress, topicName);
  }

  @Override
  public void createDestination(Datastream datastream) {
    String destination = datastream.getDestination().getConnectionString();
    int partition = datastream.getDestination().getPartitions();
    createTopic(destination, partition, new Properties());
  }

  @Override
  public void dropDestination(Datastream datastream) {
    return;
  }

  @Override
  public Duration getRetention(Datastream datastream) {
    String destination = datastream.getDestination().getConnectionString();
    return getRetention(destination);
  }

  public void createTopic(String connectionString, int numberOfPartitions, Properties topicConfig) {
    Validate.notNull(connectionString, "destination should not be null");
    Validate.notNull(topicConfig, "topicConfig should not be null");

    KafkaDestination kafkaDestination = KafkaDestination.parse(connectionString);
    String topicName = kafkaDestination.getTopicName();

    int replicationFactor = Integer.parseInt(topicConfig.getProperty("replicationFactor", DEFAULT_REPLICATION_FACTOR));
    LOG.info(
        String.format("Creating topic with name %s partitions=%d with properties %s", topicName, numberOfPartitions,
            topicConfig));

    // Add default retention if no topic-level retention is specified
    if (!topicConfig.containsKey(TOPIC_RETENTION_MS)) {
      topicConfig.put(TOPIC_RETENTION_MS, String.valueOf(_retention.toMillis()));
    }

    try {
      // Create only if it doesn't exist.
      if (!AdminUtils.topicExists(_zkUtils, topicName)) {
        AdminUtils.createTopic(_zkUtils, topicName, numberOfPartitions, replicationFactor, topicConfig, RackAwareMode.Disabled$.MODULE$);
      } else {
        LOG.warn(String.format("Topic with name %s already exists", topicName));
      }
    } catch (Throwable e) {
      LOG.error(String.format("Creating topic %s failed with exception %s ", topicName, e));
      throw e;
    }
  }

  /**
   * Consult Kafka to get the retention for a topic. This is not cached
   * in case the retention might be changed externally after creation.
   * If no topic-level retention is configured, this method returns null.
   *
   * @param destination Destination URI
   * @return topic retention or null if no such config
   */
  public Duration getRetention(String destination) {
    Validate.notNull(destination, "null destination URI");
    KafkaDestination kafkaDestination = KafkaDestination.parse(destination);
    String topicName = kafkaDestination.getTopicName();
    Properties props = AdminUtils.fetchEntityConfig(_zkUtils, ConfigType.Topic(), topicName);
    if (!props.containsKey(TOPIC_RETENTION_MS)) {
      return null;
    }
    return Duration.ofMillis(Long.parseLong(props.getProperty(TOPIC_RETENTION_MS)));
  }

  public void dropTopic(String destinationUri) {
    Validate.notNull(destinationUri, "destinationuri should not null");
    KafkaDestination kafkaDestination = KafkaDestination.parse(destinationUri);
    String topicName = kafkaDestination.getTopicName();

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
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();

    metrics.addAll(KafkaTransportProvider.getMetricInfos(_transportProviderMetricsNamesPrefix));

    return Collections.unmodifiableList(metrics);
  }

}
