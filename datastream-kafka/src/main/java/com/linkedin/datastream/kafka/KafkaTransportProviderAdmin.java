/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.TopicExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;


/**
 * {@link TransportProviderAdmin} implementation for {@link KafkaTransportProvider}
 *
 * <ul>
 *  <li>Maintains the mapping of which {@link TransportProvider} each {@link DatastreamTask} is assigned to</li>
 *  <li>Takes care of topic creation/deletion on the datastream destination</li>
 *  <li>Sets up the correct destination connection string/Kafka brokers</li>
 * </ul>
 */
public class KafkaTransportProviderAdmin implements TransportProviderAdmin {
  public static final Logger LOG = LoggerFactory.getLogger(KafkaTransportProviderAdmin.class);
  public static final int DEFAULT_PRODUCERS_PER_CONNECTOR = 10;
  public static final String DEFAULT_REPLICATION_FACTOR = "1";
  public static final String ZK_CONNECT_STRING_CONFIG = "zookeeper.connect";
  public static final String CONFIG_NUM_PRODUCERS_PER_CONNECTOR = "numProducersPerConnector";
  public static final String CONFIG_PRODUCERS_PER_TASK = "producersPerTask";
  public static final String CONFIG_METRICS_NAMES_PREFIX = "metricsNamesPrefix";
  public static final String DOMAIN_TOPIC = "topic";
  public static final String MIN_INSYNC_REPLICAS_CONFIG = "min.insync.replicas";
  public static final String TOPIC_RETENTION_MS = "retention.ms";
  public static final Duration DEFAULT_RETENTION = Duration.ofDays(14);

  private static final int DEFAULT_NUMBER_PARTITIONS = 1;
  private static final String DEFAULT_MIN_INSYNC_REPLICAS_CONFIG_VALUE = "2";
  private static final String METADATA_KAFKA_BROKERS = DatastreamMetadataConstants.SYSTEM_DESTINATION_PREFIX + "KafkaBrokers";
  private static final int GET_RETENTION_RETRY_PERIOD_MS = 2000;
  private static final int GET_RETENTION_RETRY_TIMEOUT_MS = 10000;

  private final String _transportProviderMetricsNamesPrefix;
  private final int _numProducersPerConnector;
  private final int _defaultNumProducersPerTask;
  private final Properties _transportProviderProperties;
  private final Properties _topicProperties;
  // Brokers config may not exist if transport provider handles multiple destination clusters
  private final Optional<String> _brokersConfig;
  private final Optional<String> _zkAddress;

  private final Map<DatastreamTask, KafkaTransportProvider> _transportProviders = new HashMap<>();

  // List of Kafka producers per connector-destination (broker address) pair.
  // The numProducersPerConnector config is actually the number of producers per connector-destination pair, if the
  // transport provider handles multiple destination brokers.
  private final Map<String, Map<String, List<KafkaProducerWrapper<byte[], byte[]>>>> _kafkaProducers = new HashMap<>();

  /**
   * Constructor for KafkaTransportProviderAdmin.
   * @param transportProviderName transport provider name
   * @param props TransportProviderAdmin configuration properties, e.g. ZooKeeper connection string, bootstrap.servers.
   */
  public KafkaTransportProviderAdmin(String transportProviderName, Properties props) {
    _transportProviderProperties = props;
    VerifiableProperties transportProviderProperties = new VerifiableProperties(_transportProviderProperties);

    // ZK connect string and bootstrap servers configs might not exist for connectors that manage their own destinations
    _zkAddress = Optional.ofNullable(_transportProviderProperties.getProperty(ZK_CONNECT_STRING_CONFIG))
        .filter(v -> !v.isEmpty());

    //Load default producer bootstrap server from config if available
    _brokersConfig =
        Optional.ofNullable(_transportProviderProperties.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

    _numProducersPerConnector =
        transportProviderProperties.getInt(CONFIG_NUM_PRODUCERS_PER_CONNECTOR, DEFAULT_PRODUCERS_PER_CONNECTOR);

    _defaultNumProducersPerTask = transportProviderProperties.getInt(CONFIG_PRODUCERS_PER_TASK, 1);
    org.apache.commons.lang3.Validate.isTrue(_defaultNumProducersPerTask > 0 && _defaultNumProducersPerTask <= _numProducersPerConnector,
        "Invalid value for " + CONFIG_PRODUCERS_PER_TASK);

    String metricsPrefix = transportProviderProperties.getString(CONFIG_METRICS_NAMES_PREFIX, null);
    if (metricsPrefix != null && !metricsPrefix.endsWith(".")) {
      _transportProviderMetricsNamesPrefix = metricsPrefix + ".";
    } else {
      _transportProviderMetricsNamesPrefix = metricsPrefix;
    }

    _topicProperties = transportProviderProperties.getDomainProperties(DOMAIN_TOPIC);
  }

  @Override
  public TransportProvider assignTransportProvider(DatastreamTask task) {
    Validate.notNull(task, "null task");
    if (!_transportProviders.containsKey(task)) {
      String connectorType = task.getConnectorType();
      String destinationBrokers = getDestinationBrokers(task.getDatastreams().get(0));
      if (!_kafkaProducers.containsKey(connectorType) || !_kafkaProducers.get(connectorType).containsKey(destinationBrokers)) {
        initializeKafkaProducersForConnectorDestination(connectorType, destinationBrokers);
      }
      List<KafkaProducerWrapper<byte[], byte[]>> producers =
          getNextKafkaProducers(connectorType, destinationBrokers, numProducersPerTask(task));

      Properties transportProviderProperties = new Properties();
      transportProviderProperties.putAll(_transportProviderProperties);
      transportProviderProperties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationBrokers);
      _transportProviders.put(task,
          new KafkaTransportProvider(task, producers, transportProviderProperties, _transportProviderMetricsNamesPrefix));
      producers.forEach(p -> p.assignTask(task));
    } else {
      LOG.warn("Trying to assign transport provider to task {} which is already assigned.", task);
    }

    return _transportProviders.get(task);
  }

  private void initializeKafkaProducersForConnectorDestination(String connectorType, String destinationBrokers) {
    Properties transportProviderProperties = new Properties();
    transportProviderProperties.putAll(_transportProviderProperties);
    transportProviderProperties.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, destinationBrokers);
    List<KafkaProducerWrapper<byte[], byte[]>> producers = IntStream.range(0, _numProducersPerConnector)
        .mapToObj(x -> new KafkaProducerWrapper<byte[], byte[]>(String.format("%s:%s", connectorType, x), transportProviderProperties,
            _transportProviderMetricsNamesPrefix))
        .collect(Collectors.toList());
    _kafkaProducers.putIfAbsent(connectorType, new HashMap<>());
    _kafkaProducers.get(connectorType).putIfAbsent(destinationBrokers, new ArrayList<>());
    _kafkaProducers.get(connectorType).get(destinationBrokers).addAll(producers);
  }

  @Override
  public void unassignTransportProvider(DatastreamTask task) {
    Validate.notNull(task, "null task");
    if (_transportProviders.containsKey(task)) {
      KafkaTransportProvider transportProvider = _transportProviders.remove(task);
      transportProvider.setUnassigned();
      transportProvider.getProducers().forEach(p -> p.unassignTask(task));
    } else {
      LOG.warn("Trying to unassign already unassigned transport provider.");
    }
  }

  @Override
  public void unassignTransportProvider(List<DatastreamTask> taskList) {
    Validate.notNull(taskList, "null task list");
    Set<KafkaProducerWrapper<byte[], byte[]>> producers = new HashSet<>();
    for (DatastreamTask task : taskList) {
      if (_transportProviders.containsKey(task)) {
        KafkaTransportProvider transportProvider = _transportProviders.remove(task);
        transportProvider.setUnassigned();
        producers.addAll(transportProvider.getProducers());
      } else {
        LOG.warn("Trying to unassign already unassigned transport provider for task {}.", task);
      }
    }

    producers.forEach(p -> p.unassignTasks(taskList));
  }

  @Override
  public void initializeDestinationForDatastream(Datastream datastream, String destinationName) {
    if (!datastream.hasDestination()) {
      datastream.setDestination(new DatastreamDestination());
    }

    DatastreamDestination destination = datastream.getDestination();
    DatastreamSource source = datastream.getSource();

    //always populate broker metadata if its presented
    _brokersConfig.ifPresent(brokers ->
      datastream.getMetadata().put(METADATA_KAFKA_BROKERS, brokers));

    //destination follow the hierarchy -> brokersconfg -> individual metadata -> task connection str
    if (!destination.hasConnectionString() || destination.getConnectionString().isEmpty()) {
      destination.setConnectionString(
          getDestination(datastream, destinationName));
    }

    // Skip the destination partition validation for datastreams that have connector-managed destinations
    // (i.e. mirroring connectors)
    if (!DatastreamUtils.isConnectorManagedDestination(datastream) && (!destination.hasPartitions()
        || destination.getPartitions() <= 0)) {
      if (source.hasPartitions()) {
        destination.setPartitions(source.getPartitions());
      } else {
        LOG.warn("Unable to set the number of partitions in a destination, set to default {}", DEFAULT_NUMBER_PARTITIONS);
        destination.setPartitions(DEFAULT_NUMBER_PARTITIONS);
      }
    }
  }

  @Override
  public void createDestination(Datastream datastream) {
    String destination = datastream.getDestination().getConnectionString();
    int partition = datastream.getDestination().getPartitions();
    createTopic(destination, partition, new Properties(), datastream);
  }

  @Override
  public void dropDestination(Datastream datastream) {
    LOG.info("Drop destination called for datastream {}. Ignoring it.", datastream);
  }

  /**
   * Consult Kafka to get the retention for a topic. This is not cached
   * in case the retention is changed externally after creation.
   * If no topic-level retention is configured, this method returns null.
   *
   * @param datastream Datastream
   * @return topic retention or null if no such config
   */
  @Override
  public Duration getRetention(Datastream datastream) {

    AdminClient adminClient = getAdminClient(datastream);

    String destination = datastream.getDestination().getConnectionString();
    Validate.notNull(destination, "null destination URI");
    String topicName = KafkaTransportProviderUtils.getTopicName(destination);

    // In rare cases it may happen that even though topic has been created, its metadata hasn't synced
    // to all the brokers yet and adminClient might query such a broker in which case topic retention will
    // not be present in the topic config. Therefore we retry for a few times before giving up.
    return PollUtils.poll(() -> {
      try {
        ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Map<ConfigResource, Config> topicConfigMap =
            adminClient.describeConfigs(Collections.singletonList(topicResource)).all().get();
        Config topicConfig = topicConfigMap.get(topicResource);
        ConfigEntry entry = topicConfig.get(TOPIC_RETENTION_MS);
        if (entry != null) {
          return Duration.ofMillis(Long.parseLong(entry.value()));
        }
      } catch (ExecutionException e) {
        LOG.warn("Failed to retrieve config for topic {}.", topicName, e);
      }
      return null;
    }, Objects::nonNull, GET_RETENTION_RETRY_PERIOD_MS, GET_RETENTION_RETRY_TIMEOUT_MS).orElse(null);
  }

  /**
   * Create Kafka topic based on the destination connection string, if it does not already exist.
   * @param connectionString connection string from which to obtain topic name
   * @param numberOfPartitions number of partitions
   * @param topicProperties topic config to use for topic creation
   * @param datastream the Datastream object for which to create the topic
   */
  public void createTopic(String connectionString, int numberOfPartitions, Properties topicProperties, Datastream datastream) {
    Validate.notNull(connectionString, "destination should not be null");
    Validate.notNull(topicProperties, "topicConfig should not be null");

    String topicName = KafkaTransportProviderUtils.getTopicName(connectionString);
    populateTopicConfig(topicProperties);

    try {
      AdminClient adminClient = getAdminClient(datastream);
      NewTopic newTopic = getTopic(topicName, numberOfPartitions, topicProperties);
      // Removing from topic config since its passed as a direct argument
      topicProperties.remove("replicationFactor");
      // TopicExistsException is thrown if topic exists. Its a no-op.
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        LOG.warn("Topic with name {} already exists", topicName);
      } else {
        LOG.error(String.format("Creating topic %s failed with exception: ", topicName), e);
        throw new DatastreamRuntimeException(e);
      }
    } catch (Throwable e) {
      LOG.error("Creating topic {} failed with exception {}", topicName, e);
      throw e;
    }
  }

  private AdminClient getAdminClient(Datastream datastream) {
    String destinationBrokers = getDestinationBrokers(datastream);
    if (destinationBrokers.isEmpty()) {
      throw new DatastreamRuntimeException("Destination brokers not provided for creating topic");
    }

    Properties adminClientProps = new Properties();
    adminClientProps.putAll(_transportProviderProperties);
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, destinationBrokers);
    AdminClient adminClient = AdminClient.create(adminClientProps);
    return adminClient;
  }

  private NewTopic getTopic(String topicName, int numberOfPartitions, Properties topicProperties) {
    int replicationFactor = Integer.parseInt(topicProperties.getProperty("replicationFactor", DEFAULT_REPLICATION_FACTOR));
    Map<String, String> newTopicConfig = getTopicConfig(topicProperties);
    NewTopic newTopic = new NewTopic(topicName, numberOfPartitions, (short) replicationFactor);
    newTopic.configs(newTopicConfig);
    return newTopic;
  }

  private Map<String, String> getTopicConfig(Properties topicConfig) {
    Map<String, String> newTopicConfig = new HashMap<>();
    for (String topicConfigKey : topicConfig.stringPropertyNames()) {
      newTopicConfig.put(topicConfigKey, topicConfig.getProperty(topicConfigKey));
    }
    return newTopicConfig;
  }

  private void populateTopicConfig(Properties topicConfig) {
    for (String topicConfigKey : _topicProperties.stringPropertyNames()) {
      if (!topicConfig.containsKey(topicConfigKey)) {
        topicConfig.put(topicConfigKey, _topicProperties.getProperty(topicConfigKey));
      }
    }

    if (!topicConfig.containsKey(TOPIC_RETENTION_MS)) {
      topicConfig.put(TOPIC_RETENTION_MS, String.valueOf(DEFAULT_RETENTION.toMillis()));
    }

    if (!topicConfig.containsKey(MIN_INSYNC_REPLICAS_CONFIG)) {
      topicConfig.put(MIN_INSYNC_REPLICAS_CONFIG, DEFAULT_MIN_INSYNC_REPLICAS_CONFIG_VALUE);
    }
  }

  private List<KafkaProducerWrapper<byte[], byte[]>> getNextKafkaProducers(String connectorType, String destinationBrokers, int count) {
    // Return the least used Kafka producers.
    return _kafkaProducers.get(connectorType)
        .get(destinationBrokers)
        .stream()
        .sorted(Comparator.comparingInt(KafkaProducerWrapper::getTasksSize))
        .limit(count)
        .collect(Collectors.toList());
  }

  private int numProducersPerTask(DatastreamTask task) {
    Set<Integer> values = task.getDatastreams()
        .stream()
        .map(d -> d.getMetadata().get(CONFIG_PRODUCERS_PER_TASK))
        .filter(Objects::nonNull)
        .map(Integer::valueOf)
        .filter(numProducersPerTask -> numProducersPerTask > 0)
        .collect(Collectors.toSet());

    return values.size() == 1 ? values.iterator().next() : _defaultNumProducersPerTask;
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(KafkaTransportProvider.getMetricInfos(_transportProviderMetricsNamesPrefix));
    metrics.addAll(KafkaProducerWrapper.getMetricDetails(_transportProviderMetricsNamesPrefix));
    return Collections.unmodifiableList(metrics);
  }

  /**
   * Get the Kafka destination URI for a given {@link Datastream} object
   * @param datastream the Datastream object for which to return the destination
   * @param topicName the topic name for which to return the destination
   * @return Kafka destination URI as a string
   */
  public String getDestination(Datastream datastream, String topicName) {
    String destinationBrokers = datastream == null ? null
        : datastream.getMetadata().get(KafkaDatastreamMetadataConstants.DESTINATION_KAFKA_BROKERS);
    if (destinationBrokers != null) {
      return new KafkaDestination(destinationBrokers, topicName, false).toString();
    }

    return _zkAddress.map(addr -> new KafkaDestination(addr, topicName, false).toString())
        .orElseThrow(() -> new DatastreamRuntimeException("broker is missing when generating the destination"));
  }

  //Override sequence, brokerCfg > metadata broker > task connection str
  private String getDestinationBrokers(Datastream datastream) {
    Optional<String> metadataBroker =
        Optional.ofNullable(datastream.getMetadata().get(METADATA_KAFKA_BROKERS));
    return _brokersConfig.orElse(metadataBroker.orElse(
        KafkaDestination.parse(datastream.getDestination().getConnectionString()).getZkAddress()));
  }


}
