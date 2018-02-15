package com.linkedin.datastream.connectors.kafka.mirrormaker;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.server.DatastreamTaskStatus;
import com.google.common.annotations.VisibleForTesting;
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
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
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
  // Key to get the paused partitions from datastream metadata
  public static final String MM_PAUSED_SOURCE_PARTITIONS_METADATA_KEY = "mm.pausedSourcePartitions";
  // Regex indicating pausing all partitions in a topic
  public static final String MM_REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC = "*";
  // This type reference will be used when converting paused partitions to/from Json
  public static final TypeReference<ConcurrentHashMap<String, HashSet<String>>> MM_PAUSED_SOURCE_PARTITIONS_JSON_MAP =
      new TypeReference<ConcurrentHashMap<String, HashSet<String>>>() {
      };

  // Enum indicating if there is any change in datastreak task
  // Note: This is to handle if multiple updates happen simultaneously.
  public enum TaskUpdateType {
    // Indicates no update
    NO_CHANGE,
    PAUSE_RESUME_PARTITIONS;
 }

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
  private final HashSet<TaskUpdateType> _taskUpdates = new HashSet<>(Arrays.asList(TaskUpdateType.PAUSE_RESUME_PARTITIONS));

  // This variable is used only for testing
  @VisibleForTesting
  int _pausedPartitionsUpdatedCount = 0;

  // stores source partitions that are paused for given datastream.
  // Note: always use with a lock on pausedPartitionsUpdated
  private Map<String, HashSet<String>> _pausedSourcePartitions = new ConcurrentHashMap<>();

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
    // TODO: detect when new topic falls into subscription and create topic in destination.
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(CommonConnectorMetrics.getEventProcessingMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getEventPollMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getPartitionSpecificMetrics(METRICS_PREFIX_REGEX));
    return metrics;
  }

  @Override
  public void run() {
    _logger.info("Starting the Kafka-based connector task for {}", _datastreamTask);
    boolean startingUp = true;
    long pollInterval = 0; // so 1st call to poll is fast for purposes of startup
    _thread = Thread.currentThread();

    _producer = _datastreamTask.getEventProducer();
    _eventsProcessedCountLoggedTime = Instant.now();

    try {
      try (Consumer<?, ?> consumer = createKafkaConsumer(_consumerProps)) {
        _consumer = consumer;
        consumerSubscribe();

        ConsumerRecords<?, ?> records;
        while (!_shouldDie) {
          // first check if need to update the list of paused partitions
          checkAndPausePartitions();

          // read a batch of records
          records = pollRecords(pollInterval);

          // handle startup notification if this is the 1st poll call
          if (startingUp) {
            pollInterval = getPollIntervalMs();
            startingUp = false;
            _startedLatch.countDown();
          }

          if (records != null && !records.isEmpty()) {
            Instant readTime = Instant.now();
            processRecords(records, readTime);
            trackEventsProcessedProgress(records.count());
          }
        } // end while loop

        // shutdown
        maybeCommitOffsets(consumer, true);
      }
    } catch (Exception e) {
      _logger.error("{} failed with exception.", _taskName, e);
      _datastreamTask.setStatus(DatastreamTaskStatus.error(e.getMessage()));
      throw new DatastreamRuntimeException(e);
    } finally {
      _stoppedLatch.countDown();
      _logger.info("{} stopped", _taskName);
    }
  }

  @Override
  public void checkAndUpdateTask(DatastreamTask datastreamTask) {
    synchronized (_taskUpdates) {
      //todo gaurav: should _datastreamTask also be updated? We will need synchronization around it, if so.

      // first get the given set of paused source partitions from metadata
      String pausedPartitionsJson =
          datastreamTask.getDatastreams().get(0).getMetadata().get(MM_PAUSED_SOURCE_PARTITIONS_METADATA_KEY);

      // convert the json to actual map of topic, source partitions
      Map<String, HashSet<String>> newPausedSourcePartitionsMap = new ConcurrentHashMap<>();
      if (!pausedPartitionsJson.isEmpty()) {
        newPausedSourcePartitionsMap = JsonUtils.fromJson(pausedPartitionsJson, MM_PAUSED_SOURCE_PARTITIONS_JSON_MAP);
      }

      if (!newPausedSourcePartitionsMap.equals(_pausedSourcePartitions)) {
        _pausedSourcePartitions = newPausedSourcePartitionsMap;
        _taskUpdates.add(TaskUpdateType.PAUSE_RESUME_PARTITIONS);
      }
    }
  }

  // This method checks if there was any change in paused partitions
  private void checkAndPausePartitions() {
    synchronized (_taskUpdates) {
      if (_taskUpdates.contains(TaskUpdateType.PAUSE_RESUME_PARTITIONS)) {
        Validate.isTrue(_consumer != null);

        LOG.info(String.format("List of partitions to pause changed for datastream: %s. The list is now: %s",
            _datastream.getName(), _pausedSourcePartitions));

        // contains list of new partitions to pause
        Set<TopicPartition> partitionsToPause = new HashSet<>();

        // contains list of partitions that are now unpaused and should be resumed
        Set<TopicPartition> partitionsToResume = new HashSet<>();

        // get the config that's already there on the consumer
        Set<TopicPartition> pausedPartitions = _consumer.paused();
        Set<TopicPartition> assignedPartitions = _consumer.assignment();

        // unpause previously paused partitions that are no longer paused.
        // For this, check the set of partitions that are paused already.
        // if they are not a part of current set of paused partitions, put them in
        // partitionsToResume
        if (pausedPartitions.size() > 0) {
          for (TopicPartition tp : pausedPartitions) {
            // if the new set of paused partitions contains the topic partition
            // or if all the partitions belonging to a topic are to be paused
            // (denoted by MM_REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC), keep the partition paused
            if (_pausedSourcePartitions.containsKey(tp.topic()) && (
                _pausedSourcePartitions.get(tp.topic()).contains(MM_REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC)
                    || _pausedSourcePartitions.get(tp.topic()).contains(Integer.toString(tp.partition())))) {
              continue;
            } else {
              partitionsToResume.add(tp);
            }
          }
        }

        // Now check the current assignment.
        // see if any partition is a part of new paused partition list, and if they are not
        // already paused. If not, add them to partitionsToPause list.
        if (assignedPartitions.size() > 0) {
          for (TopicPartition tp : assignedPartitions) {
            // if the new set of paused partitions contains the topic partition
            // or if all the partitions belonging to a topic are to be paused
            // (denoted by MM_REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC), add it to the list to
            // the list of partitions to pause (if not paused already)
            if (_pausedSourcePartitions.containsKey(tp.topic()) && (
                _pausedSourcePartitions.get(tp.topic()).contains(MM_REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC)
                    || _pausedSourcePartitions.get(tp.topic()).contains(Integer.toString(tp.partition())))
                && !pausedPartitions.contains(tp)) {
              partitionsToPause.add(tp);
            }
          }
        }

        // todo gaurav: do we need this?
        maybeCommitOffsets(_consumer, true);

        // resume partitions to resume
        _consumer.resume(partitionsToResume);
        LOG.info(String.format("Resumed these partitions: %s", partitionsToResume));

        // pause partitions to pause
        _consumer.pause(partitionsToPause);
        LOG.info(String.format("Paused these partitions: %s", partitionsToPause));

        _taskUpdates.remove(TaskUpdateType.PAUSE_RESUME_PARTITIONS);

        // This is only for testing purpose.
        _pausedPartitionsUpdatedCount++;
      }
    }
  }

  @VisibleForTesting
  int getPausedPartitionsUpdateCount() {
    Integer ret = 0;
    // Make sure no update on going
    synchronized (_taskUpdates) {
      ret = _pausedPartitionsUpdatedCount;
    }
    return ret;
  }

  @VisibleForTesting
  Map<String, HashSet<String>> getPausedSourcePartitions() {
    Map<String, HashSet<String>> pausedSourcePartitions = new ConcurrentHashMap<>();
    // Make sure there is no on going update.
    synchronized (_taskUpdates) {
      pausedSourcePartitions.putAll(_pausedSourcePartitions);
    }
    return pausedSourcePartitions;
  }
}
