/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.helix.zookeeper.zkclient.exception.ZkInterruptedException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.TopicPartitionUtil;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaBasedConnectorTask;
import com.linkedin.datastream.connectors.kafka.GroupIdConstructor;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig;
import com.linkedin.datastream.connectors.kafka.KafkaBrokerAddress;
import com.linkedin.datastream.connectors.kafka.KafkaConnectionString;
import com.linkedin.datastream.connectors.kafka.KafkaDatastreamStatesResponse;
import com.linkedin.datastream.connectors.kafka.PausedSourcePartitionMetadata;
import com.linkedin.datastream.kafka.factory.KafkaConsumerFactory;
import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;
import com.linkedin.datastream.server.NoOpTransportProviderAdminFactory;
import com.linkedin.datastream.server.api.transport.SendCallback;


/**
 * KafkaMirrorMakerConnectorTask consumes from Kafka using regular expression pattern subscription. This means that the
 * task is consuming from multiple topics at once. When a new topic falls into the subscription, the task should
 * create the topic in the destination before attempting to produce to it.
 *
 * This task is responsible for specifying the destination for every DatastreamProducerRecord it sends downstream. As
 * such, the Datastream destination connection string should be a format String, where "%s" should be replaced by the
 * specific topic to send to.
 *
 * If flushless mode is enabled, the task will not invoke flush on the producer unless shutdown is requested. In
 * flushless mode, task keeps track of the safe checkpoints for each source partition and only commits offsets that were
 * acknowledged in the producer callback. Flow control can only be used in flushless mode.
 */
public class KafkaMirrorMakerConnectorTask extends AbstractKafkaBasedConnectorTask {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMirrorMakerConnectorTask.class.getName());
  private static final String CLASS_NAME = KafkaMirrorMakerConnectorTask.class.getSimpleName();

  private static final String KAFKA_ORIGIN_CLUSTER = "kafka-origin-cluster";
  private static final String KAFKA_ORIGIN_TOPIC = "kafka-origin-topic";
  private static final String KAFKA_ORIGIN_PARTITION = "kafka-origin-partition";
  private static final String KAFKA_ORIGIN_OFFSET = "kafka-origin-offset";
  private static final Duration LOCK_ACQUIRE_TIMEOUT = Duration.ofMinutes(3);
  private static final String TASK_LOCK_ACQUIRE_ERROR_RATE = "taskLockAcquireErrorRate";
  private static final String DATASTREAM_NAME_BASED_CLIENT_ID_FORMAT = "%s-%s";

  // constants for flushless mode and flow control
  protected static final String CONFIG_MAX_IN_FLIGHT_MSGS_THRESHOLD = "maxInFlightMessagesThreshold";
  protected static final String CONFIG_MIN_IN_FLIGHT_MSGS_THRESHOLD = "minInFlightMessagesThreshold";
  protected static final String CONFIG_FLOW_CONTROL_ENABLED = "flowControlEnabled";
  private static final long DEFAULT_MAX_IN_FLIGHT_MSGS_THRESHOLD = 5000;
  private static final long DEFAULT_MIN_IN_FLIGHT_MSGS_THRESHOLD = 1000;
  private static final String DEFAULT_DESTINATION_TOPIC_PREFIX = "";

  // constants for topic manager
  public static final String TOPIC_MANAGER_FACTORY = "topicManagerFactory";
  public static final String DEFAULT_TOPIC_MANAGER_FACTORY =
      "com.linkedin.datastream.connectors.kafka.mirrormaker.NoOpTopicManagerFactory";
  public static final String DESTINATION_TOPIC_PREFIX = "destinationTopicPrefix";
  public static final String DOMAIN_TOPIC_MANAGER = "topicManager";
  public static final String TOPIC_MANAGER_METRICS_PREFIX = "TopicManager";

  protected final DynamicMetricsManager _dynamicMetricsManager;
  protected final String _connectorName;

  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final KafkaConnectionString _mirrorMakerSource;

  // Topic manager can be used to handle topic related tasks that mirror maker connector needs to do.
  // Topic manager is invoked every time there is a new partition assignment (for both partitions assigned and revoked),
  // and also before every poll call.
  private final TopicManager _topicManager;

  // variables for flushless mode and flow control
  private final boolean _isFlushlessModeEnabled;
  private final boolean _isIdentityMirroringEnabled;
  private final boolean _enablePartitionAssignment;
  // If enabled, this appends the datastream name to the consumer client.id. This can be useful to differentiate
  // among Kafka consumer client metrics for different datastreams.
  private final boolean _includeDatastreamNameInConsumerClientId;
  private final String _destinationTopicPrefix;
  private final long _hardCommitFlushTimeoutMs;
  private FlushlessEventProducerHandler<Long> _flushlessProducer = null;
  private boolean _flowControlEnabled = false;
  private long _maxInFlightMessagesThreshold;
  private long _minInFlightMessagesThreshold;
  private int _flowControlTriggerCount = 0;
  private int _errorOnSendCallbackDuringShutdownCount = 0;
  private ExecutorService _producerFlushExecutor;

  /**
   * Constructor for KafkaMirrorMakerConnectorTask
   * @param config Task configuration properties
   * @param task Datastream task
   * @param connectorName Connector name
   * @param isFlushlessModeEnabled true if {@value KafkaMirrorMakerConnector#IS_FLUSHLESS_MODE_ENABLED}
   *                               is set to true for the connector identified with {@code connectorName}
   * @param groupIdConstructor Kafka consumer group ID constructor
   */
  public KafkaMirrorMakerConnectorTask(KafkaBasedConnectorConfig config, DatastreamTask task, String connectorName,
      boolean isFlushlessModeEnabled, GroupIdConstructor groupIdConstructor) {
    super(config, task, LOG, generateMetricsPrefix(connectorName, CLASS_NAME), groupIdConstructor);
    _consumerFactory = config.getConsumerFactory();
    _mirrorMakerSource = KafkaConnectionString.valueOf(_datastreamTask.getDatastreamSource().getConnectionString());

    _isFlushlessModeEnabled = isFlushlessModeEnabled;
    _connectorName = connectorName;
    _isIdentityMirroringEnabled = KafkaMirrorMakerDatastreamMetadata.isIdentityPartitioningEnabled(_datastream);
    _enablePartitionAssignment = config.getEnablePartitionAssignment();
    _includeDatastreamNameInConsumerClientId = config.getIncludeDatastreamNameInConsumerClientId();
    _hardCommitFlushTimeoutMs = config.getHardCommitFlushTimeoutMs();
    _destinationTopicPrefix = task.getDatastreams().get(0).getMetadata()
        .getOrDefault(DatastreamMetadataConstants.DESTINATION_TOPIC_PREFIX, DEFAULT_DESTINATION_TOPIC_PREFIX);
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _producerFlushExecutor = Executors.newSingleThreadExecutor();

    if (_enablePartitionAssignment) {
      LOG.info("Enable Brooklin partition assignment");
    }

    LOG.info("Destination topic prefix has been set to {}", _destinationTopicPrefix);

    if (_isFlushlessModeEnabled) {
      _flushlessProducer = new FlushlessEventProducerHandler<Long>(_producer,
          config.getCallbackStatusStrategyFactory());
      _flowControlEnabled = config.getConnectorProps().getBoolean(CONFIG_FLOW_CONTROL_ENABLED, false);
      _maxInFlightMessagesThreshold =
          config.getConnectorProps().getLong(CONFIG_MAX_IN_FLIGHT_MSGS_THRESHOLD, DEFAULT_MAX_IN_FLIGHT_MSGS_THRESHOLD);
      _minInFlightMessagesThreshold =
          config.getConnectorProps().getLong(CONFIG_MIN_IN_FLIGHT_MSGS_THRESHOLD, DEFAULT_MIN_IN_FLIGHT_MSGS_THRESHOLD);
      LOG.info("Flushless mode is enabled for task: {}, with flowControlEnabled={}, minInFlightMessagesThreshold={}, "
              + "maxInFlightMessagesThreshold={}", task.getDatastreamTaskName(), _flowControlEnabled,
          _minInFlightMessagesThreshold, _maxInFlightMessagesThreshold);
    }

    // create topic manager
    // by default it creates a NoOpTopicManager.
    VerifiableProperties connectorProperties = config.getConnectorProps();
    Properties topicManagerProperties = new Properties();
    String topicManagerFactoryName = DEFAULT_TOPIC_MANAGER_FACTORY;
    if (null != connectorProperties &&
        null != connectorProperties.getProperty(TOPIC_MANAGER_FACTORY)) {
      topicManagerProperties = connectorProperties.getDomainProperties(DOMAIN_TOPIC_MANAGER);
      topicManagerFactoryName = connectorProperties.getProperty(TOPIC_MANAGER_FACTORY);
      // Propagate the destinationTopicPrefix config to the topic manager so that it can create destination topics
      // with the same prefix.
      topicManagerProperties.put(DESTINATION_TOPIC_PREFIX, _destinationTopicPrefix);
    }

    TopicManagerFactory topicManagerFactory = ReflectionUtils.createInstance(topicManagerFactoryName);
    _topicManager =
        topicManagerFactory.createTopicManager(_datastreamTask, _datastream, _groupIdConstructor, _consumerFactory,
            _consumerProps, topicManagerProperties, _consumerMetrics,
            generateMetricsPrefix(connectorName, CLASS_NAME) + TOPIC_MANAGER_METRICS_PREFIX, _datastreamName);
  }

  @Override
  protected Consumer<?, ?> createKafkaConsumer(Properties consumerProps) {
    Properties properties = new Properties();
    properties.putAll(consumerProps);
    String bootstrapValue = _mirrorMakerSource.getBrokers().stream().map(KafkaBrokerAddress::toString)
        .collect(Collectors.joining(KafkaConnectionString.BROKER_LIST_DELIMITER));
    properties.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapValue);
    properties.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        Boolean.FALSE.toString()); // auto-commits are unsafe
    properties.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_AUTO_OFFSET_RESET_CONFIG_EARLIEST);
    properties.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG,
        getKafkaGroupId(_datastreamTask, _groupIdConstructor, _consumerMetrics, LOG));
    if (_includeDatastreamNameInConsumerClientId) {
      String clientId = properties.getProperty(ConsumerConfig.CLIENT_ID_CONFIG, "");
      properties.put(ConsumerConfig.CLIENT_ID_CONFIG,
          String.format(DATASTREAM_NAME_BASED_CLIENT_ID_FORMAT, clientId, _datastreamName));
    }
    LOG.info("Creating Kafka consumer for task {} with properties {}, include datastream name in client.id: {}",
        _datastreamTask, properties, _includeDatastreamNameInConsumerClientId);
    return _consumerFactory.createConsumer(properties);
  }

  @Override
  protected void consumerSubscribe() {
    if (_enablePartitionAssignment) {
      Set<TopicPartition> topicPartition  = getAssignedTopicPartitionFromTask();
      updateConsumerAssignment(topicPartition);
      // Consumer can be assigned with an empty set, but it cannot run poll against it
      // While it's allowed to assign an empty set here, the check on poll need to be performed
      _consumer.assign(_consumerAssignment);
      this.onPartitionsAssignedInternal(topicPartition);
      // Invoke topic manager
      handleTopicMangerPartitionAssignment(topicPartition);
    } else {
      LOG.info("About to subscribe to source: {}", _mirrorMakerSource.getTopicName());
      _consumer.subscribe(Pattern.compile(_mirrorMakerSource.getTopicName()), this);
    }
  }

  private Set<TopicPartition> getAssignedTopicPartitionFromTask() {
    return _datastreamTask.getPartitionsV2().stream()
        .map(TopicPartitionUtil::createTopicPartition).collect(Collectors.toSet());
  }

  @Override
  protected DatastreamProducerRecord translate(ConsumerRecord<?, ?> fromKafka, Instant readTime) {
    long eventsSourceTimestamp =
        fromKafka.timestampType() == TimestampType.LOG_APPEND_TIME ? fromKafka.timestamp() : readTime.toEpochMilli();
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put(KAFKA_ORIGIN_CLUSTER, _mirrorMakerSource.getBrokerListString());
    String topic = fromKafka.topic();
    metadata.put(KAFKA_ORIGIN_TOPIC, topic);
    int partition = fromKafka.partition();
    String partitionStr = String.valueOf(partition);
    metadata.put(KAFKA_ORIGIN_PARTITION, partitionStr);
    long offset = fromKafka.offset();
    String offsetStr = String.valueOf(offset);
    metadata.put(KAFKA_ORIGIN_OFFSET, offsetStr);
    metadata.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(eventsSourceTimestamp));
    metadata.put(BrooklinEnvelopeMetadataConstants.SOURCE_PARTITION, partitionStr);
    BrooklinEnvelope envelope = new BrooklinEnvelope(fromKafka.key(), fromKafka.value(), null,
        fromKafka.headers(), metadata);
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(envelope);
    builder.setEventsSourceTimestamp(eventsSourceTimestamp);
    builder.setSourceCheckpoint(new KafkaMirrorMakerCheckpoint(topic, partition, offset).toString());

    if (!_datastreamTask.getTransportProviderName().
        equalsIgnoreCase(NoOpTransportProviderAdminFactory.NoOpTransportProvider.NAME)) {
      builder.setDestination(_datastreamTask.getDatastreamDestination()
          .getConnectionString()
          .replace(KafkaMirrorMakerConnector.MM_TOPIC_PLACEHOLDER,
              StringUtils.isBlank(_destinationTopicPrefix) ? topic : _destinationTopicPrefix + topic));
    }
    if (_isIdentityMirroringEnabled) {
      builder.setPartition(partition);
    }
    return builder.build();
  }

  @Override
  protected void sendDatastreamProducerRecord(DatastreamProducerRecord datastreamProducerRecord,
      TopicPartition srcTopicPartition, int numBytes, SendCallback sendCallback) {
    if (_isFlushlessModeEnabled) {
      // The topic/partition from checkpoint is the same as srcTopicPartition
      KafkaMirrorMakerCheckpoint sourceCheckpoint =
          new KafkaMirrorMakerCheckpoint(datastreamProducerRecord.getCheckpoint());
      String topic = sourceCheckpoint.getTopic();
      int partition = sourceCheckpoint.getPartition();
      try {
        _flushlessProducer.send(datastreamProducerRecord, topic, partition, sourceCheckpoint.getOffset(), ((metadata, exception) -> {
          if (exception != null) {
            String msg = String.format("Detected exception being thrown from flushless send callback for source "
                + "topic-partition: %s with metadata: %s, exception: %s", srcTopicPartition, metadata, exception);
            if (_shutdown) {
              if (_errorOnSendCallbackDuringShutdownCount == 0) {
                LOG.warn(msg);
              } else {
                LOG.debug(msg);
              }
              _errorOnSendCallbackDuringShutdownCount++;
            } else {
              LOG.warn(msg);
            }
            updateSendFailureTopicPartitionExceptionMap(srcTopicPartition, exception);
          } else {
            _consumerMetrics.updateBytesProcessedRate(numBytes);
          }
          if (sendCallback != null) {
            sendCallback.onCompletion(metadata, exception);
          }
        }));
      } catch (Exception e) {
        LOG.warn("Hit Exception while sending records for {}-{}, total inFlightMessageCount: {}, ackMessagesPastCheckpoint: {}",
            topic, partition, _flushlessProducer.getInFlightMessagesCounts(), _flushlessProducer.getAckMessagesPastCheckpointCounts());
        maybeCommitOffsets(_consumer, true);
        throw e;
      }
      if (_flowControlEnabled) {
        TopicPartition tp = new TopicPartition(topic, partition);
        long inFlightMessageCount = _flushlessProducer.getInFlightCount(topic, partition);
        if (inFlightMessageCount > _maxInFlightMessagesThreshold) {
          // add the partition to the pause list
          LOG.warn(
              "In-flight message count of {} for topic partition {} exceeded maxInFlightMessagesThreshold of {}. Will pause partition.",
              inFlightMessageCount, tp, _maxInFlightMessagesThreshold);
          _autoPausedSourcePartitions.put(tp, new PausedSourcePartitionMetadata(
              () -> _flushlessProducer.getInFlightCount(topic, partition) <= _minInFlightMessagesThreshold,
              PausedSourcePartitionMetadata.Reason.EXCEEDED_MAX_IN_FLIGHT_MSG_THRESHOLD));
          _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
          _flowControlTriggerCount++;
        }
      }
    } else {
      super.sendDatastreamProducerRecord(datastreamProducerRecord, srcTopicPartition, numBytes, sendCallback);
    }
  }

  private void handleTopicMangerPartitionAssignment(Collection<TopicPartition> partitions) {
    Collection<TopicPartition> topicPartitionsToPause = _topicManager.onPartitionsAssigned(partitions);
    // we need to explicitly pause these partitions here and not wait for PreConsumerPollHook.
    // The reason being onPartitionsAssigned() gets called as part of Kafka poll, so we need to pause partitions
    // before that poll call can return any data.
    // If we let partitions be paused as part of pre-poll hook, that will happen in the next poll cycle.
    // Chances are that the current poll call will return data already for that topic/partition and we will end up
    // auto creating that topic.
    pauseTopicManagerPartitions(topicPartitionsToPause);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    super.onPartitionsAssigned(partitions);
    handleTopicMangerPartitionAssignment(partitions);
  }

  @Override
  public void run() {
    if (_enablePartitionAssignment) {
      try {
        LOG.info("Trying to acquire the lock on datastreamTask: {}", _datastreamTask);
        _datastreamTask.acquire(LOCK_ACQUIRE_TIMEOUT);
      } catch (DatastreamRuntimeException ex) {
        // setting _stoppedLatch count to 0 since the lock couldn't be acquired,
        // as a non-zero stoppedLatch value won't let the task to be stopped.
        countDownStoppedLatch();
        LOG.error(String.format("Failed to acquire lock for datastreamTask %s", _datastreamTask), ex);
        _dynamicMetricsManager.createOrUpdateMeter(generateMetricsPrefix(_connectorName, CLASS_NAME), _datastreamName,
            TASK_LOCK_ACQUIRE_ERROR_RATE, 1);
        // This exception should not be swallowed as it is fatal and can cause multiple instances
        // to work on the same task concurrently
        throw ex;
      }
    }
    super.run();
  }

  @Override
  public void stop() {
    super.stop();
    _topicManager.stop();
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
    if (getSkipOnPartitionsRevoked()) {
      _logger.info("Skipping commit in onPartitionsRevoked during consumer.close()");
      return;
    }
    super.onPartitionsRevoked(partitions);
    _topicManager.onPartitionsRevoked(partitions);
  }

  private void commitSafeOffsets(Consumer<?, ?> consumer) {
    LOG.info("Trying to commit safe offsets.");
    Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
    for (TopicPartition tp : consumer.assignment()) {
      // add 1 to the last acked checkpoint to set to the offset of the next message to consume
      _flushlessProducer.getAckCheckpoint(tp.topic(), tp.partition())
          .ifPresent(o -> offsets.put(tp, new OffsetAndMetadata(o + 1)));
    }
    commitWithRetries(consumer, Optional.of(offsets));
    _lastCommittedTime = System.currentTimeMillis();
  }

  @Override
  protected void maybeCommitOffsets(Consumer<?, ?> consumer, boolean hardCommit) {
    boolean isTimeToCommit = System.currentTimeMillis() - _lastCommittedTime > _offsetCommitInterval;
    if (_isFlushlessModeEnabled) {
      if (hardCommit) { // hard commit (flush and commit checkpoints)
        LOG.info("Calling flush on the producer.");
        try {
          Future<?> producerFlushFuture = _producerFlushExecutor.submit(() -> _datastreamTask.getEventProducer().flush());
          producerFlushFuture.get(_hardCommitFlushTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (Exception ex) {
          LOG.warn("Producer flush failed with exception: ", ex);
        } finally {
          // Flushless mode tracks the successfully received acks, so it is safe to commit offsets even if flush throws
          // an exception. Commit the safe offsets to reduce send duplication.
          commitSafeOffsets(consumer);
        }
        // clear the flushless producer state after flushing all messages and checkpointing
        _flushlessProducer.clear();
      } else if (isTimeToCommit) { // soft commit (no flush, just commit checkpoints)
        commitSafeOffsets(consumer);
      }
    } else {
      super.maybeCommitOffsetsInternal(consumer, hardCommit);
    }
  }

  @Override
  protected ConsumerRecords<?, ?> consumerPoll(long pollInterval) {
    if (_enablePartitionAssignment && _consumerAssignment.isEmpty()) {
      // Kafka rejects a poll if there is empty assignment
      return ConsumerRecords.EMPTY;
    } else {
      return _consumer.poll(pollInterval);
    }
  }

  @Override
  protected void postShutdownHook() {
    LOG.info("Total send callback errors during shutdown: {}", _errorOnSendCallbackDuringShutdownCount);
    if (_enablePartitionAssignment) {
      boolean resetInterrupted = false;
      try {
        for (int numAttempts = 1; numAttempts <= 3; ++numAttempts) {
          try {
            // The task lock should only be released when it is absolutely safe (we can guarantee that the task cannot
            // consume any further). The shutdown process must complete and the consumer must be closed.
            LOG.info("Releasing the lock on datastreamTask: {}, was thread interrupted: {}, attempt: {}",
                _datastreamTask, resetInterrupted, numAttempts);
            _datastreamTask.release();
            break;
          } catch (ZkInterruptedException e) {
            LOG.warn("Releasing the task lock failed for datastreamTask: {}, retrying", _datastreamTask);
            if (Thread.currentThread().isInterrupted()) {
              // The interrupted status of the current thread must be reset to allow the task lock to be released
              resetInterrupted = Thread.interrupted();
            }
          }
        }
      } finally {
        if (resetInterrupted) {
          // Setting the status of the thread back to interrupted
          Thread.currentThread().interrupt();
        }
      }
    }
    _producerFlushExecutor.shutdown();
  }

  /**
   * Get all metrics info for the connector whose name is {@code connectorName}
   */
  public static List<BrooklinMetricInfo> getMetricInfos(String connectorName) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(AbstractKafkaBasedConnectorTask.getMetricInfos(
        generateMetricsPrefix(connectorName, CLASS_NAME) + MetricsAware.KEY_REGEX));
    metrics.add(new BrooklinMeterInfo(generateMetricsPrefix(connectorName, CLASS_NAME) + MetricsAware.KEY_REGEX
        + TASK_LOCK_ACQUIRE_ERROR_RATE));
    // As topic manager is plugged in as part of task creation, one can't know
    // what topic manager is being used (and hence metrics topic manager emits), until task is initiated.
    // Hack is all topic managers should use same prefix for metrics they emit (TOPIC_MANAGER_METRICS_PREFIX)
    // and return here different types of metrics with that prefix.
    // for every metrics type emitted by Topic manager, it should be added here.
    // Any metrics emitted should match exactly one metrics regex otherwise it results in Sensor errors.
    metrics.add(new BrooklinCounterInfo(generateMetricsPrefix(connectorName, CLASS_NAME) + TOPIC_MANAGER_METRICS_PREFIX
        + "." + TopicManager.COUNTER + MetricsAware.KEY_REGEX + ".*"));
    metrics.add(new BrooklinGaugeInfo(generateMetricsPrefix(connectorName, CLASS_NAME) + TOPIC_MANAGER_METRICS_PREFIX
        + "." + TopicManager.GAUGE + MetricsAware.KEY_REGEX + ".*"));
    return metrics;
  }

  /**
   * This method pauses topics that are returned by topic manager.
   * We need to explicitly pause these partitions here and not wait for PreConsumerPollHook.
   * The reason being onPartitionsAssigned() gets called as part of Kafka poll, so we need to pause partitions
   * before that poll call can return any data.
   * If we let partitions be paused as part of pre-poll hook, that will happen in the next poll cycle.
   * Chances are that the current poll call will return data already for that topic/partition and we will end up
   * auto creating that topic.
   */
  private void pauseTopicManagerPartitions(Collection<TopicPartition> topicManagerPartitions) {
    if (null == topicManagerPartitions || topicManagerPartitions.isEmpty()) {
      return;
    }

    _consumer.pause(topicManagerPartitions);
    for (TopicPartition tp : topicManagerPartitions) {
      _autoPausedSourcePartitions.put(tp,
          new PausedSourcePartitionMetadata(() -> _topicManager.shouldResumePartition(tp),
              PausedSourcePartitionMetadata.Reason.TOPIC_NOT_CREATED));
    }
  }

  @Override
  public KafkaDatastreamStatesResponse getKafkaDatastreamStatesResponse() {
    return new KafkaDatastreamStatesResponse(_datastreamName, _autoPausedSourcePartitions, _pausedPartitionsConfig,
        _consumerAssignment,
        _isFlushlessModeEnabled ? _flushlessProducer.getInFlightMessagesCounts() : Collections.emptyMap());
  }

  @VisibleForTesting
  protected void seekToLastCheckpoint(Set<TopicPartition> topicPartitions) {
    try {
      super.seekToLastCheckpoint(topicPartitions);
    } catch (Exception e) {
      commitSafeOffsets(_consumer);
      throw e;
    } finally {
      if (_isFlushlessModeEnabled) {
        topicPartitions.forEach(topicPartition -> _flushlessProducer.clear(topicPartition.topic(), topicPartition.partition()));
      }
    }
  }

  @Override
  protected void getLastCheckpointToSeekTo(Map<TopicPartition, OffsetAndMetadata> lastCheckpoint,
      Set<TopicPartition> tpWithNoCommits, TopicPartition tp) {
    if (_isFlushlessModeEnabled) {
      // Flushless mode tracks the successfully received acks, so it is safe to rewind to that offsets rather than
      // last committed offset
      _flushlessProducer.getAckCheckpoint(tp.topic(), tp.partition())
          .ifPresent(o -> lastCheckpoint.put(tp, new OffsetAndMetadata(o + 1)));
      if (!lastCheckpoint.containsKey(tp)) {
        super.getLastCheckpointToSeekTo(lastCheckpoint, tpWithNoCommits, tp);
      }
    } else {
      super.getLastCheckpointToSeekTo(lastCheckpoint, tpWithNoCommits, tp);
    }
  }

  @VisibleForTesting
  long getInFlightMessagesCount(String source, int partition) {
    return _isFlushlessModeEnabled ? _flushlessProducer.getInFlightCount(source, partition) : 0;
  }

  @VisibleForTesting
  int getFlowControlTriggerCount() {
    return _flowControlTriggerCount;
  }

  @VisibleForTesting
  public TopicManager getTopicManager() {
    return _topicManager;
  }
}
