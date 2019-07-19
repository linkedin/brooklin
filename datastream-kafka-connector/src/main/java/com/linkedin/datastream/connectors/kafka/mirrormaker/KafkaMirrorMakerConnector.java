/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import java.util.stream.Collectors;

import org.apache.commons.collections.ListUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamPartitionsMetadata;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaBasedConnectorTask;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaConnector;
import com.linkedin.datastream.connectors.kafka.KafkaBrokerAddress;
import com.linkedin.datastream.connectors.kafka.KafkaConnectionString;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactory;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


/**
 * KafkaMirrorMakerConnector is similar to KafkaConnector but it has the ability to consume from multiple topics in a
 * cluster via regular expression pattern source, and it has the ability to produce to multiple topics in the
 * destination cluster.
 */
public class KafkaMirrorMakerConnector extends AbstractKafkaConnector {
  protected static final String IS_FLUSHLESS_MODE_ENABLED = "isFlushlessModeEnabled";
  protected static final String PARTITION_FETCH_INTERVAL = "PartitionFetchIntervalMs";
  protected static final String MM_TOPIC_PLACEHOLDER = "*";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMirrorMakerConnector.class);
  private static final String DEST_CONSUMER_GROUP_ID_SUFFIX = "-topic-partition-listener";
  private static final String DOMAIN_KAFKA_CONSUMER = "consumer";
  private static final long DEFAULT_PARTITION_FETCH_INTERVAL = 30000;

  private final boolean _isFlushlessModeEnabled;
  private final long _partitionFetchIntervalMs;
  private final KafkaConsumerFactory<?, ?> _listenerConsumerFactory;
  private final Map<String, PartitionDiscoveryThread> _partitionDiscoveryThreadMap = new HashMap<>();
  private final Properties _consumerProperties;
  private java.util.function.Consumer<DatastreamGroup> _partitionChangeCallback;
  private boolean _shutdown;

  /**
   * Constructor for KafkaMirrorMakerConnector.
   * @param connectorName Name of the KafkaMirrorMakerConnector.
   * @param config Config to use while creating the instance of KafkaMirrorMakerConnector.
   * @param clusterName Name of Brooklin cluster where connector will be running
   */
  public KafkaMirrorMakerConnector(String connectorName, Properties config, String clusterName) {
    super(connectorName, config, new KafkaMirrorMakerGroupIdConstructor(
            Boolean.parseBoolean(config.getProperty(IS_GROUP_ID_HASHING_ENABLED, Boolean.FALSE.toString())), clusterName),
        clusterName, LOG);
    _isFlushlessModeEnabled =
        Boolean.parseBoolean(config.getProperty(IS_FLUSHLESS_MODE_ENABLED, Boolean.FALSE.toString()));
    _partitionFetchIntervalMs = Long.parseLong(config.getProperty(PARTITION_FETCH_INTERVAL,
        Long.toString(DEFAULT_PARTITION_FETCH_INTERVAL)));
    VerifiableProperties verifiableProperties = new VerifiableProperties(config);
    _consumerProperties = verifiableProperties.getDomainProperties(DOMAIN_KAFKA_CONSUMER);
    _listenerConsumerFactory = new KafkaConsumerFactoryImpl();
    _shutdown = false;
  }

  @Override
  protected AbstractKafkaBasedConnectorTask createKafkaBasedConnectorTask(DatastreamTask task) {
    return new KafkaMirrorMakerConnectorTask(_config, task, _connectorName, _isFlushlessModeEnabled,
        _groupIdConstructor);
  }

  @Override
  public void initializeDatastream(Datastream stream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {

    // verify that the MirrorMaker Datastream will not be re-used
    if (DatastreamUtils.isReuseAllowed(stream)) {
      throw new DatastreamValidationException(
          String.format("Destination reuse is not allowed for connector %s. Datastream: %s", stream.getConnectorName(),
              stream));
    }

    // verify that BYOT is not used
    if (DatastreamUtils.isUserManagedDestination(stream)) {
      throw new DatastreamValidationException(
          String.format("BYOT is not allowed for connector %s. Datastream: %s", stream.getConnectorName(), stream));
    }

    if (!DatastreamUtils.isConnectorManagedDestination(stream)) {
      stream.getMetadata()
          .put(DatastreamMetadataConstants.IS_CONNECTOR_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());
    }

    // verify that the source regular expression can be compiled
    KafkaConnectionString connectionString = KafkaConnectionString.valueOf(stream.getSource().getConnectionString());
    try {
      Pattern pattern = Pattern.compile(connectionString.getTopicName());
      LOG.info("Successfully compiled topic name pattern {}", pattern);
    } catch (PatternSyntaxException e) {
      throw new DatastreamValidationException(
          String.format("Regular expression in Datastream source connection string (%s) is ill-formatted.",
              stream.getSource().getConnectionString()), e);
    }
  }

  @Override
  public String getDestinationName(Datastream stream) {
    // return topic placeholder string so that topic can be inserted into the destination string at produce time
    return MM_TOPIC_PLACEHOLDER;
  }

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return Collections.unmodifiableList(KafkaMirrorMakerConnectorTask.getMetricInfos(_connectorName));
  }

  @Override
  public void postDatastreamInitialize(Datastream datastream, List<Datastream> allDatastreams)
      throws DatastreamValidationException {
    _groupIdConstructor.populateDatastreamGroupIdInMetadata(datastream, allDatastreams, Optional.of(LOG));
  }

  @Override
  public void onPartitionChange(java.util.function.Consumer<DatastreamGroup> callback) {
    _partitionChangeCallback = callback;
  }

  @Override
  public void stop() {
    super.stop();
    _shutdown = true;
    _partitionDiscoveryThreadMap.values().forEach(Thread::interrupt);
  }

  @Override
  public Map<String, Optional<DatastreamPartitionsMetadata>> getDatastreamPartitions() {
    Map<String, Optional<DatastreamPartitionsMetadata>> datastreams = new HashMap<>();
    _partitionDiscoveryThreadMap.forEach((s, partitionDiscoveryThread) -> {
      if (partitionDiscoveryThread._initialized) {
        datastreams.put(s, Optional.of(new DatastreamPartitionsMetadata(s,
            partitionDiscoveryThread._subscribedPartitions)));
      } else {
        datastreams.put(s, Optional.empty());
      }
    });
    return datastreams;
  }


  @Override
  public void handleDatastream(List<DatastreamGroup> datastreamGroups) {
    List<String> dgNames = datastreamGroups.stream().map(DatastreamGroup::getTaskPrefix).collect(Collectors.toList());
    List<String> obsoleteDgs = new ArrayList<>(_partitionDiscoveryThreadMap.keySet());
    obsoleteDgs.removeAll(dgNames);
    obsoleteDgs.stream().forEach(name -> {
      LOG.info("remove datastream group {}", name);
      Optional.ofNullable(_partitionDiscoveryThreadMap.remove(name)).ifPresent(Thread::interrupt);
    });

    datastreamGroups.stream().forEach(datastreamGroup -> {
      String datastreamGroupName = datastreamGroup.getTaskPrefix();
      PartitionDiscoveryThread partitionDiscoveryThread;
      if (_partitionDiscoveryThreadMap.containsKey(datastreamGroupName)) {
        partitionDiscoveryThread = _partitionDiscoveryThreadMap.get(datastreamGroupName);
        partitionDiscoveryThread.setDatastreamGroup(datastreamGroup);
      } else {
        //We dont need to register the thread if no partition callback is available
        if (_partitionChangeCallback != null) {
          partitionDiscoveryThread =
              new PartitionDiscoveryThread(datastreamGroup);
          partitionDiscoveryThread.start();
          _partitionDiscoveryThreadMap.put(datastreamGroupName, partitionDiscoveryThread);
          LOG.info("DatastreamChangeListener for {} registered", datastreamGroupName);
        }
      }
    });
  }

  class PartitionDiscoveryThread extends Thread {
    private Consumer<?, ?> _consumer;
    private DatastreamGroup _datastreamGroup;
    private List<String> _subscribedPartitions = new ArrayList<>();
    private Pattern _topicPattern;
    private boolean _initialized;


    private PartitionDiscoveryThread(DatastreamGroup datastreamGroup) {
      _datastreamGroup = datastreamGroup;
      _topicPattern = Pattern.compile(
          KafkaConnectionString.valueOf(_datastreamGroup.getDatastreams().get(0).getSource().getConnectionString()).getTopicName());
      _initialized = false;
    }

    private void setDatastreamGroup(DatastreamGroup datastreamGroup) {
      _datastreamGroup = datastreamGroup;
      _topicPattern = Pattern.compile(
          KafkaConnectionString.valueOf(_datastreamGroup.getDatastreams().get(0).getSource().getConnectionString()).getTopicName());
    }

    private List<String> getPartitionsInfo() {
      Map<String, List<PartitionInfo>> sourceTopics = _consumer.listTopics();
      List<TopicPartition> topicPartitions = sourceTopics.keySet().stream()
          .filter(t1 -> _topicPattern.matcher(t1).matches()).flatMap(t2 ->
              sourceTopics.get(t2).stream().map(partitionInfo ->
                  new TopicPartition(partitionInfo.topic(), partitionInfo.partition()))).collect(Collectors.toList());

      return topicPartitions.stream().map(TopicPartition::toString).sorted().collect(Collectors.toList());
    }

    private Consumer<?, ?> createConsumer(Properties consumerProps, String bootstrapServers, String groupId) {
      Properties properties = new Properties();
      properties.putAll(consumerProps);
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
          ByteArrayDeserializer.class.getCanonicalName());
      properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
          ByteArrayDeserializer.class.getCanonicalName());
      return _listenerConsumerFactory.createConsumer(properties);
    }

    @Override
    public void run() {
      Datastream datastream = _datastreamGroup.getDatastreams().get(0);
      String bootstrapValue = String.join(KafkaConnectionString.BROKER_LIST_DELIMITER,
          KafkaConnectionString.valueOf(datastream.getSource().getConnectionString())
              .getBrokers().stream().map(KafkaBrokerAddress::toString).collect(Collectors.toList()));
      _consumer = createConsumer(_consumerProperties, bootstrapValue,
          _groupIdConstructor.constructGroupId(datastream) + DEST_CONSUMER_GROUP_ID_SUFFIX);

      LOG.info("Fetch thread for {} started", datastream.getName());
      while (!isInterrupted() && !_shutdown) {
        try {
          // If partition is changed
          List<String> newPartitionInfo = getPartitionsInfo();
          LOG.info("Fetch partition info for {}, oldPartitionInfo: {}, new Partition info: {}"
              , datastream.getName(), _subscribedPartitions, newPartitionInfo);

          if (!ListUtils.isEqualList(newPartitionInfo, _subscribedPartitions)) {
            LOG.info("get updated partition info for {}, oldPartitionInfo: {}, new Partition info: {}"
                , datastream.getName(), _subscribedPartitions, newPartitionInfo);

            _subscribedPartitions = Collections.synchronizedList(newPartitionInfo);
            _initialized = true;
            _partitionChangeCallback.accept(_datastreamGroup);
          }
          Thread.sleep(_partitionFetchIntervalMs);
        } catch (Throwable t) {
          LOG.error("detect error for thread " + datastream.getName() + ", ex: ", t);
        }
      }

      if (_consumer != null) {
        _consumer.close();
      }
      _consumer = null;
      LOG.info("Fetch thread for {} stopped", datastream.getName());
    }
  }
}
