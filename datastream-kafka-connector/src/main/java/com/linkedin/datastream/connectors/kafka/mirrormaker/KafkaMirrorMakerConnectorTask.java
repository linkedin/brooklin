package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaBasedConnectorTask;
import com.linkedin.datastream.connectors.kafka.KafkaBrokerAddress;
import com.linkedin.datastream.connectors.kafka.KafkaConnectionString;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactory;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * KafkaMirrorMakerConnectorTask consumes from Kafka using regular expression pattern subscription. This means that the
 * task is consuming from multiple topics at once. When a new topic falls into the subscription, the task should
 * create the topic in the destination before attempting to produce to it.
 *
 * This task is responsible for specifying the destination for every DatastreamProducerRecord it sends downstream. As
 * such, the Datastream destination connection string should be a format String, where "%s" should be replaced by the
 * specific topic to send to.
 */
public class KafkaMirrorMakerConnectorTask extends AbstractKafkaBasedConnectorTask {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaMirrorMakerConnectorTask.class.getName());
  private static final String CLASS_NAME = KafkaMirrorMakerConnectorTask.class.getSimpleName();
  private static final String METRICS_PREFIX_REGEX = CLASS_NAME + MetricsAware.KEY_REGEX;

  private static final String KAFKA_ORIGIN_CLUSTER = "kafka-origin-cluster";
  private static final String KAFKA_ORIGIN_TOPIC = "kafka-origin-topic";
  private static final String KAFKA_ORIGIN_PARTITION = "kafka-origin-partition";
  private static final String KAFKA_ORIGIN_OFFSET = "kafka-origin-offset";

  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final KafkaConnectionString _mirrorMakerSource;

  // Set indicating if there is any change in task.
  // Initialize _taskUpdates to all the updates which should be set to "true"
  // at the time of startup.
  private final ConcurrentLinkedQueue<DatastreamConstants.UpdateType> _taskUpdates =
      new ConcurrentLinkedQueue<>(Arrays.asList(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS));

  // This variable is used only for testing
  @VisibleForTesting
  int _pausedPartitionsUpdatedCount = 0;

  // stores source partitions that are paused for given datastream.
  // Note: always use with a lock on pausedPartitionsUpdated
  private Map<String, Set<String>> _pausedSourcePartitions = new ConcurrentHashMap<>();

  protected KafkaMirrorMakerConnectorTask(KafkaConsumerFactory<?, ?> factory, Properties consumerProps,
      DatastreamTask task, long commitIntervalMillis, Duration retrySleepDuration, int retryCount) {
    super(consumerProps, task, commitIntervalMillis, retrySleepDuration, retryCount, LOG);
    _consumerFactory = factory;
    _mirrorMakerSource = KafkaConnectionString.valueOf(_datastreamTask.getDatastreamSource().getConnectionString());
  }

  @Override
  protected Consumer<?, ?> createKafkaConsumer(Properties consumerProps) {
    String bootstrapValue = String.join(KafkaConnectionString.BROKER_LIST_DELIMITER,
        _mirrorMakerSource.getBrokers().stream().map(KafkaBrokerAddress::toString).collect(Collectors.toList()));

    consumerProps.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapValue);
    consumerProps.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, _datastreamTask.getDatastreams().get(0).getName());
    consumerProps.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,
        Boolean.FALSE.toString()); // auto-commits are unsafe
    // TODO: read auto.offset.reset from the config or metadata so that this is configurable by SRE, who might want
    // latest when pipeline is first set up but might want to change to "oldest" for all newly created topics, once
    // pipeline is stabilized.
    consumerProps.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    return _consumerFactory.createConsumer(consumerProps);
  }

  @Override
  protected void consumerSubscribe() {
    _consumer.subscribe(Pattern.compile(_mirrorMakerSource.getTopicName()), this);
  }


  @Override
  protected DatastreamProducerRecord translate(ConsumerRecord<?, ?> fromKafka, Instant readTime) throws Exception {
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put(KAFKA_ORIGIN_CLUSTER, _mirrorMakerSource.toString());
    String topic = fromKafka.topic();
    metadata.put(KAFKA_ORIGIN_TOPIC, topic);
    int partition = fromKafka.partition();
    String partitionStr = String.valueOf(partition);
    metadata.put(KAFKA_ORIGIN_PARTITION, partitionStr);
    long offset = fromKafka.offset();
    String offsetStr = String.valueOf(offset);
    metadata.put(KAFKA_ORIGIN_OFFSET, offsetStr);
    metadata.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(readTime.toEpochMilli()));
    BrooklinEnvelope envelope = new BrooklinEnvelope(fromKafka.key(), fromKafka.value(), null, metadata);
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(envelope);
    builder.setEventsSourceTimestamp(readTime.toEpochMilli());
    builder.setSourceCheckpoint(new KafkaMirrorMakerCheckpoint(topic, partition, offset).toString());
    builder.setDestination(String.format(_task.getDatastreamDestination().getConnectionString(), topic));
    return builder.build();
  }


  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    _consumerMetrics.updateRebalanceRate(1);
    LOG.info("Partition ownership assigned for {}.", partitions);
    // update paused partitions, in case.
    _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    // TODO: detect when new topic falls into subscription and create topic in destination.
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(CommonConnectorMetrics.getEventProcessingMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getEventPollMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getPartitionSpecificMetrics(METRICS_PREFIX_REGEX));
    return metrics;
  }

  // Note: This method is supposed to be called from run() thread, so in effect, it is "single threaded".
  // The only updates that can happen to _taskUpdates queue outside this method is addition of
  // new update type when there is any update to datastream task (in method checkForUpdateTask())
  @Override
  protected void preConsumerPollHook() {
    // See if there was any update in task and take actions.
    while (!_taskUpdates.isEmpty()) {
      DatastreamConstants.UpdateType updateType = _taskUpdates.remove();
      if (updateType != null) {
        switch (updateType) {
          case PAUSE_RESUME_PARTITIONS:
            pausePartitions();
            break;
          default:
            String msg = String.format("Unknown update type {} for task {}.", updateType, _taskName);
            _logger.error(msg);
            throw new DatastreamRuntimeException(msg);
        }
      }
    }
  }

  @Override
  public void checkForUpdateTask(DatastreamTask datastreamTask) {
    // check if there was any change in paused partitions.
    checkForPausedPartitionsUpdate(datastreamTask);
  }

  // This method checks if there is any change in set of paused partitions for given datastreamtask.
  private void checkForPausedPartitionsUpdate(DatastreamTask datastreamTask) {
    // Check if there is any change in paused partitions
    // first get the given set of paused source partitions from metadata
    Map<String, Set<String>> newPausedSourcePartitionsMap = new HashMap<>();
    newPausedSourcePartitionsMap =
        DatastreamUtils.getDatastreamSourcePartitions(datastreamTask.getDatastreams().get(0));

    if (!newPausedSourcePartitionsMap.equals(_pausedSourcePartitions)) {
      _pausedSourcePartitions.clear();
      _pausedSourcePartitions.putAll(newPausedSourcePartitionsMap);
      _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    }
  }

  // TODO: Move logic to pause partitions to AbstractKafkaConnector
  // This method pauses/resumes partitions.
  private void pausePartitions() {
    // This is only for testing purpose.
    _pausedPartitionsUpdatedCount++;

    Validate.isTrue(_consumer != null);

    LOG.info("List of partitions to pause changed for datastream: {}. The list is: {}", _datastream.getName(),
        _pausedSourcePartitions);

    // contains list of new partitions to pause
    Set<TopicPartition> partitionsToPause = new HashSet<>();

    // get the config that's already there on the consumer
    Set<TopicPartition> pausedPartitions = _consumer.paused();
    Set<TopicPartition> assignedPartitions = _consumer.assignment();

    // Get partitions to pause
    for (String source : _pausedSourcePartitions.keySet()) {
      for (String partition : _pausedSourcePartitions.get(source)) {
        partitionsToPause.add(new TopicPartition(source, Integer.parseInt(partition)));
      }
    }

    // Make sure those partitions are assigned to this task.
    partitionsToPause.retainAll(assignedPartitions);

    // If the partitions to pause are paused already, don't do anything.
    if (partitionsToPause.equals(pausedPartitions)) {
      return;
    }

    // Resume current paused partitions by default.
    _consumer.resume(pausedPartitions);
    LOG.info("Resumed these partitions: {}", pausedPartitions);

    // pause partitions to pause
    _consumer.pause(partitionsToPause);
    LOG.info("Paused these partitions: {}", partitionsToPause);
  }

  @VisibleForTesting
  int getPausedPartitionsUpdateCount() {
    return _pausedPartitionsUpdatedCount;
  }

  @VisibleForTesting
  Map<String, Set<String>> getPausedSourcePartitions() {
    Map<String, Set<String>> pausedSourcePartitions = new ConcurrentHashMap<>();
    pausedSourcePartitions.putAll(_pausedSourcePartitions);
    return pausedSourcePartitions;
  }
}
