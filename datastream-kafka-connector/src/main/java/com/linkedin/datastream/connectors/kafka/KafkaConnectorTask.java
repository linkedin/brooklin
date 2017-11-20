package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.google.common.annotations.VisibleForTesting;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskStatus;


public class KafkaConnectorTask implements Runnable {
  public static final String GROUP_ID_CONFIG = "group.id";

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectorTask.class);
  private static final String CLASS_NAME = KafkaConnectorTask.class.getSimpleName();
  // Regular expression to capture all metrics by this kafka connector.
  private static final String METRICS_PREFIX_REGEX = CLASS_NAME + MetricsAware.KEY_REGEX;
  private static final long POLL_BUFFER_TIME_MS = 1000;

  public static final String CFG_SKIP_BAD_MESSAGE = "skipBadMessage";
  public static final String PROCESSING_DELAY_LOG_TRESHOLD_MS = "processingDelayLogThreshold";
  public static final long DEFAULT_PROCESSING_DELAY_LOG_TRESHOLD_MS = Duration.ofMinutes(1).toMillis();
  private final long _processingDelayLogThresholdMs;

  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final Properties _consumerProps;
  private final String _datastreamName;
  private final Datastream _datastream;
  private final Optional<Map<Integer, Long>> _startOffsets;

  //lifecycle
  private volatile boolean _shouldDie = false;
  private final CountDownLatch _startedLatch = new CountDownLatch(1);
  private final CountDownLatch _stoppedLatch = new CountDownLatch(1);
  //config
  private final DatastreamTask _task;
  private final long _offsetCommitInterval;
  private final Duration _retrySleepDuration;
  private final int _retryCount;

  //state
  private volatile Thread _thread;
  private volatile String _taskName;
  private String _srcValue;
  private DatastreamEventProducer _producer;
  private Consumer<?, ?> _consumer;

  private long _lastCommittedTime = System.currentTimeMillis();
  private final CommonConnectorMetrics _consumerMetrics;


  public KafkaConnectorTask(KafkaConsumerFactory<?, ?> factory, Properties consumerProps, DatastreamTask task,
      long commitIntervalMillis, Duration retrySleepDuration, int retryCount) {
    LOG.info("Creating kafka connector task for datastream task {} with commit interval {}Ms", task,
        commitIntervalMillis);
    _consumerProps = consumerProps;
    VerifiableProperties properties = new VerifiableProperties(_consumerProps);

    _processingDelayLogThresholdMs = properties.containsKey(PROCESSING_DELAY_LOG_TRESHOLD_MS) ?
        properties.getLong(PROCESSING_DELAY_LOG_TRESHOLD_MS) : DEFAULT_PROCESSING_DELAY_LOG_TRESHOLD_MS;

    _consumerFactory = factory;
    _datastream = task.getDatastreams().get(0);
    _task = task;
    _retryCount = retryCount;

    _startOffsets = Optional.ofNullable(_datastream.getMetadata().get(DatastreamMetadataConstants.START_POSITION))
        .map(json -> JsonUtils.fromJson(json, new TypeReference<Map<Integer, Long>>() {
        }));

    _offsetCommitInterval = commitIntervalMillis;
    _retrySleepDuration = retrySleepDuration;
    _datastreamName = task.getDatastreams().get(0).getName();

    _consumerMetrics = new CommonConnectorMetrics(CLASS_NAME, _datastreamName, LOG);
    _consumerMetrics.createEventProcessingMetrics();
    _consumerMetrics.createPollMetrics();
    _consumerMetrics.createPartitionMetrics();
    _taskName = task.getDatastreamTaskName();
  }

  public static Consumer<?, ?> createConsumer(KafkaConsumerFactory<?, ?> consumerFactory, Properties consumerProps,
      String groupId, KafkaConnectionString connectionString) {

    Properties props = getKafkaConsumerProperties(consumerProps, groupId, connectionString);
    return consumerFactory.createConsumer(props);
  }

  @VisibleForTesting
  static Properties getKafkaConsumerProperties(Properties consumerProps, String groupId,
      KafkaConnectionString connectionString) {
    StringJoiner csv = new StringJoiner(",");
    connectionString.getBrokers().forEach(broker -> csv.add(broker.toString()));
    String bootstrapValue = csv.toString();

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapValue);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false"); //auto-commits are unsafe
    props.put("auto.offset.reset", "none");
    props.put("security.protocol", connectionString.isSecure() ? "SSL" : "PLAINTEXT");
    props.putAll(consumerProps);
    return props;
  }

  @Override
  public void run() {
    LOG.info("Starting the kafka connector task for {}", _task);
    boolean startingUp = true;
    long pollInterval = 0; //so 1st call to poll is fast for purposes of startup
    _thread = Thread.currentThread();
    try {

      DatastreamSource source = _task.getDatastreamSource();
      KafkaConnectionString srcConnString = KafkaConnectionString.valueOf(source.getConnectionString());

      _srcValue = srcConnString.toString();
      _producer = _task.getEventProducer();
      String kafkaGroupId = getKafkaGroupId(_task);

      try (Consumer<?, ?> consumer = createConsumer(_consumerFactory, _consumerProps, kafkaGroupId, srcConnString)) {
        _consumer = consumer;
        ConsumerRecords<?, ?> records;
        consumer.subscribe(Collections.singletonList(srcConnString.getTopicName()), new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.info("Partition ownership revoked for {}, checkpointing.", partitions);
            if (!_shouldDie) { // There is a commit at the end of the run method, skip extra commit in shouldDie mode.
              maybeCommitOffsets(consumer, true); //happens inline as part of poll
            }
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            _consumerMetrics.updateRebalanceRate(1);
            //nop
            LOG.info("Partition ownership assigned for {}.", partitions);
          }
        });

        while (!_shouldDie) {
          //read a batch of records
          try {
            long curPollTime = System.currentTimeMillis();
            records = consumer.poll(pollInterval);
            long pollDurationMs = System.currentTimeMillis() - curPollTime;
            if (pollDurationMs > pollInterval + POLL_BUFFER_TIME_MS) {
              // Record poll time exceeding client poll timeout
              LOG.warn(String.format("ConsumerId: %s, Kafka client poll took %d ms (> poll timeout %d + buffer time %d ms)",
                  _taskName, pollDurationMs, pollInterval, POLL_BUFFER_TIME_MS));
              _consumerMetrics.updateClientPollOverTimeout(1);
            }
            _consumerMetrics.updateNumPolls(1);
            _consumerMetrics.updateEventCountsPerPoll(records.count());
            if (!records.isEmpty()) {
              _consumerMetrics.updateEventsProcessedRate(records.count());
              _consumerMetrics.updateLastEventReceivedTime(Instant.now());
            }
          } catch (NoOffsetForPartitionException e) {
            LOG.info("Poll threw NoOffsetForPartitionException for partitions {}.", e.partitions());
            if (_shouldDie) {
              continue;
            }
            seekToStartPosition(consumer, e.partitions());
            continue;
          } catch (WakeupException e) {
            LOG.warn("Got a Wakeup Exception, shutdown in progress {}.", e);
            continue;

          } catch (Exception e) {
            LOG.warn("Poll threw an exception. Sleeping for {} seconds and retrying. Exception: {}",
                _retrySleepDuration.getSeconds(), e);
            if (_shouldDie) {
              continue;
            }
            Thread.sleep(_retrySleepDuration.toMillis());
            continue;
          }

          //handle startup notification if this is the 1st poll call
          if (startingUp) {
            pollInterval = _offsetCommitInterval / 2; //leave time for processing, assume 50-50
            startingUp = false;
            _startedLatch.countDown();
          }

          try {

            //send the batch out the other end
            long readTime = System.currentTimeMillis();
            String strReadTime = String.valueOf(readTime);
            // TODO(misanchez): we should have a way to signal the producer to stop and throw an exception
            //                  in case the _shouldDie signal is set (similar to kafka wakeup)
            translateAndSendBatch(records, readTime, strReadTime);

            if (System.currentTimeMillis() - readTime > _processingDelayLogThresholdMs) {
              _consumerMetrics.updateProcessingAboveThreshold(1);
            }

            if (_shouldDie) {
              continue;
            }

            //potentially commit our offsets (if its been long enough and all sends were successful)
            maybeCommitOffsets(consumer, false);
          } catch (Exception e) {
            LOG.info("sending the messages failed with exception: {}", e);
            if (_shouldDie) {
              LOG.warn("Exiting without commit, not point to try to recover in shouldDie mode.");
              return; // Return to skip the commit after the while loop.
            }
            LOG.info("Trying to start processing from previous checkpoint.");
            Map<TopicPartition, OffsetAndMetadata> lastCheckpoint = new HashMap<>();
            Set<TopicPartition> tpWithNoCommits = new HashSet<>();
            //construct last checkpoint
            consumer.assignment().forEach(tp -> {
              OffsetAndMetadata offset =  consumer.committed(tp);
              // offset can be null if there was no prior commit
              if (offset == null) {
                tpWithNoCommits.add(tp);
              } else {
                lastCheckpoint.put(tp, consumer.committed(tp));
              }
            });
            LOG.info("Seeking to previous checkpoints {} and start.", lastCheckpoint);
            //reset consumer to last checkpoint
            lastCheckpoint.forEach((tp, offsetAndMetadata) -> consumer.seek(tp, offsetAndMetadata.offset()));
            if (!tpWithNoCommits.isEmpty()) {
              LOG.info("Seeking to start position {} and start ", tpWithNoCommits);
              seekToStartPosition(consumer, tpWithNoCommits);
            }
          }
        }

        //shutdown
        maybeCommitOffsets(consumer, true);
      }
    } catch (Exception e) {
      LOG.error("{} failed with exception.", _taskName, e);
      _task.setStatus(DatastreamTaskStatus.error(e.getMessage()));
      throw new DatastreamRuntimeException(e);
    } finally {
      _stoppedLatch.countDown();
      LOG.info("{} stopped", _taskName);
    }
  }

  @VisibleForTesting
  static String getKafkaGroupId(DatastreamTask task) {
    KafkaConnectionString srcConnString =
        KafkaConnectionString.valueOf(task.getDatastreamSource().getConnectionString());
    String dstConnString = task.getDatastreamDestination().getConnectionString();

    return task.getDatastreams()
        .stream()
        .map(ds -> ds.getMetadata().get(GROUP_ID_CONFIG))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(srcConnString + "-to-" + dstConnString);
  }

  private void seekToStartPosition(Consumer<?, ?> consumer, Set<TopicPartition> partitions) {
    if (_startOffsets.isPresent()) {
      LOG.info("Datastream is configured with StartPosition. Trying to start from {}",
          _startOffsets.get());
      seekToOffset(consumer, partitions, _startOffsets.get());
    } else {
      //means we have no saved offsets for some partitions, reset it to latest for those
      consumer.seekToEnd(partitions);
    }
  }

  private static void seekToOffset(Consumer<?, ?> consumer, Set<TopicPartition> partitions,
      Map<Integer, Long> startOffsets) {
    partitions.forEach(tp -> {
      Long offset = startOffsets.get(tp.partition());
      if (offset == null) {
        String msg = String.format("Couldn't find the offset for partition %s", tp);
        LOG.error(msg);
        throw new DatastreamRuntimeException(msg);
      }
      consumer.seek(tp, offset);
    });

    LOG.info("Seek completed to the offsets.");
  }

  public void stop() {
    LOG.info("{} stopping", _taskName);
    _shouldDie = true;
    if (_consumer != null) {
      _consumer.wakeup();
    }
    _thread.interrupt();
  }

  public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
    return _startedLatch.await(timeout, unit);
  }

  public boolean awaitStop(long timeout, TimeUnit unit) throws InterruptedException {
    return _stoppedLatch.await(timeout, unit);
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(CommonConnectorMetrics.getEventProcessingMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getEventPollMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getPartitionSpecificMetrics(METRICS_PREFIX_REGEX));
    return metrics;
  }

  private void maybeCommitOffsets(Consumer<?, ?> consumer, boolean force) {
    long now = System.currentTimeMillis();
    long timeSinceLastCommit = now - _lastCommittedTime;
    if (force || timeSinceLastCommit > _offsetCommitInterval) {
      LOG.info("Trying to flush the producer and commit offsets.");
      _producer.flush();
      consumer.commitSync();
      _lastCommittedTime = System.currentTimeMillis();
    }
  }

  private void translateAndSendBatch(ConsumerRecords<?, ?> records, long readTime, String strReadTime)
      throws InterruptedException {
    for (ConsumerRecord<?, ?> record : records) {
      try {
        sendMessage(record, readTime, strReadTime);
      } catch (RuntimeException e) {
        _consumerMetrics.updateErrorRate(1);
        // Throw the exception and let the connector rewind and retry.
        throw e;
      }
    }
  }

  private void sendMessage(ConsumerRecord<?, ?> record, long readTime, String strReadTime) throws InterruptedException {
    int count = 0;
    while (true) {
      count++;
      try {
        int numBytes = record.serializedKeySize() + record.serializedValueSize();
        _consumerMetrics.updateBytesProcessedRate(numBytes);
        _producer.send(translate(record, readTime, strReadTime), null);
        return; // Break the retry loop and exit.
      } catch (RuntimeException e) {
        LOG.error("Error sending Message. task: {} ; error: {};", _taskName, e.toString());
        LOG.error("Stack Trace: {}", Arrays.toString(e.getStackTrace()));
        if (_shouldDie || count >= _retryCount) {
          LOG.info("Send messages failed with exception: {}", e);
          throw e;
        }
        LOG.warn("Sleeping for {} seconds before retrying. Retry {} of {}",
            _retrySleepDuration.getSeconds(), count, _retryCount);
        Thread.sleep(_retrySleepDuration.toMillis());
      }
    }
  }

  private DatastreamProducerRecord translate(ConsumerRecord<?, ?> fromKafka, long readTime, String strReadTime) {
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put("kafka-origin", _srcValue);
    int partition = fromKafka.partition();
    String partitionStr = String.valueOf(partition);
    metadata.put("kafka-origin-partition", partitionStr);
    String offsetStr = String.valueOf(fromKafka.offset());
    metadata.put("kafka-origin-offset", offsetStr);
    metadata.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, strReadTime);
    BrooklinEnvelope envelope = new BrooklinEnvelope(fromKafka.key(), fromKafka.value(), null, metadata);
    //TODO - copy over headers if/when they are ever supported
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(envelope);
    builder.setEventsSourceTimestamp(readTime);
    builder.setPartition(partition); //assume source partition count is same as dest
    builder.setSourceCheckpoint(partitionStr + "-" + offsetStr);

    return builder.build();
  }
}
