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
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SlidingTimeWindowReservoir;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskStatus;


public class KafkaConnectorTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectorTask.class);
  private static final String CLASS_NAME = KafkaConnectorTask.class.getSimpleName();

  public static final String CFG_SKIP_BAD_MESSAGE = "skipBadMessage";

  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final Properties _consumerProps;
  private final DynamicMetricsManager _dynamicMetricsManager;
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
  private final boolean _skipBadMessagesEnabled;
  private final int _retryCount;

  //state
  private volatile Thread _thread;
  private volatile String _taskName;
  private String _srcValue;
  private DatastreamEventProducer _producer;
  private Consumer<?, ?> _consumer;

  private static final String EVENTS_PROCESSED_RATE = "eventsProcessedRate";
  private static final String EVENTS_BYTE_PROCESSED_RATE = "eventsByteProcessedRate";
  private static final String AGGREGATE = "aggregate";
  private static final String ERROR_RATE = "errorRate";
  private static final String SKIPPED_BAD_MESSAGES_RATE = "skippedBadMessagesRate";
  private static final String REBALANCE_RATE = "rebalanceRate";
  private static final String NUM_KAFKA_POLLS = "numKafkaPolls";
  private static final String EVENT_COUNTS_PER_POLL = "eventCountsPerPoll";
  private static final String TIME_SINCE_LAST_EVENT_RECEIVED = "timeSinceLastEventReceivedMs";
  private static final String TIME_SINCE_LAST_EVENT_PROCESSED = "timeSinceLastEventProcessedMs";

  // Regular expression to capture all metrics by this kafka connector.
  private static final String METRICS_PREFIX_REGEX = CLASS_NAME + MetricsAware.KEY_REGEX;

  private Instant _lastEventReceivedTime = Instant.now();
  private Instant _lastEventProcessedTime = Instant.now();
  // Per consumer metrics
  private final Meter _eventsProcessedRate = new Meter();
  private final Meter _bytesProcessedRate = new Meter();
  private final Meter _errorRate = new Meter();
  private final Meter _skippedBadMessagesRate = new Meter();
  private final Meter _rebalanceRate = new Meter();
  private final Meter _numKafkaPolls = new Meter();
  private final Gauge<Long> _timeSinceLastEventReceived =
      () -> Duration.between(Instant.now(), _lastEventReceivedTime).toMillis();
  private final Gauge<Long> _timeSinceLastEventProcessed =
      () -> Duration.between(Instant.now(), _lastEventProcessedTime).toMillis();
  private final Histogram _eventCountsPerPoll = new Histogram(new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES));

  // Aggregated metrics
  private static final Meter AGGREGATED_EVENTS_PROCESSED_RATE = new Meter();
  private static final Meter AGGREGATED_BYTES_PROCESSED_RATE = new Meter();
  private static final Meter AGGREGATED_ERROR_RATE = new Meter();
  private static final Meter AGGREGATED_REBALANCE_RATE = new Meter();
  private long _lastCommittedTime = System.currentTimeMillis();

  public KafkaConnectorTask(KafkaConsumerFactory<?, ?> factory, Properties consumerProps, DatastreamTask task,
      long commitIntervalMillis, Duration retrySleepDuration, int retryCount) {
    LOG.info("Creating kafka connector task for datastream task {} with commit interval {}Ms", task,
        commitIntervalMillis);
    _consumerProps = consumerProps;
    _consumerFactory = factory;
    _datastream = task.getDatastreams().get(0);
    _task = task;
    _skipBadMessagesEnabled = skipBadMessageEnabled(task);
    _retryCount = retryCount;

    _startOffsets = Optional.ofNullable(_datastream.getMetadata().get(DatastreamMetadataConstants.START_POSITION))
        .map(json -> JsonUtils.fromJson(json, new TypeReference<Map<Integer, Long>>() {
        }));

    _offsetCommitInterval = commitIntervalMillis;
    _retrySleepDuration = retrySleepDuration;
    _datastreamName = task.getDatastreams().get(0).getName();

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();

    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, EVENTS_PROCESSED_RATE, _eventsProcessedRate);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, EVENTS_BYTE_PROCESSED_RATE, _bytesProcessedRate);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, ERROR_RATE, _errorRate);
    if (_skipBadMessagesEnabled) {
      _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, SKIPPED_BAD_MESSAGES_RATE,
          _skippedBadMessagesRate);
    }

    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, REBALANCE_RATE, _rebalanceRate);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, NUM_KAFKA_POLLS, _numKafkaPolls);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, EVENT_COUNTS_PER_POLL, _eventCountsPerPoll);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, TIME_SINCE_LAST_EVENT_RECEIVED,
        _timeSinceLastEventReceived);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, TIME_SINCE_LAST_EVENT_PROCESSED,
        _timeSinceLastEventProcessed);

    // Register Aggregated metric if not register by another instance.

    _dynamicMetricsManager.registerMetric(CLASS_NAME, AGGREGATE, EVENTS_PROCESSED_RATE,
        AGGREGATED_EVENTS_PROCESSED_RATE);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, AGGREGATE, EVENTS_BYTE_PROCESSED_RATE,
        AGGREGATED_BYTES_PROCESSED_RATE);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, AGGREGATE, ERROR_RATE, AGGREGATED_ERROR_RATE);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, AGGREGATE, REBALANCE_RATE, AGGREGATED_REBALANCE_RATE);
  }

  public static Consumer<?, ?> createConsumer(KafkaConsumerFactory<?, ?> consumerFactory, Properties consumerProps,
      String groupId, KafkaConnectionString connectionString) {

    StringJoiner csv = new StringJoiner(",");
    connectionString.getBrokers().forEach(broker -> csv.add(broker.toString()));
    String bootstrapValue = csv.toString();

    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapValue);
    props.put("group.id", groupId);
    props.put("enable.auto.commit", "false"); //auto-commits are unsafe
    props.put("auto.offset.reset", "none");
    props.putAll(consumerProps);
    return consumerFactory.createConsumer(props);
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

      DatastreamDestination destination = _task.getDatastreamDestination();
      String dstConnString = destination.getConnectionString();
      _producer = _task.getEventProducer();
      _taskName = srcConnString + "-to-" + dstConnString;

      try (Consumer<?, ?> consumer = createConsumer(_consumerFactory, _consumerProps, _taskName, srcConnString)) {
        _consumer = consumer;
        ConsumerRecords<?, ?> records;
        consumer.subscribe(Collections.singletonList(srcConnString.getTopicName()), new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.trace("Partition ownership revoked for {}, checkpointing.", partitions);
            if (!_shouldDie) { // There is a commit at the end of the run method, skip extra commit in shouldDie mode.
              maybeCommitOffsets(consumer, true); //happens inline as part of poll
            }
          }

          @Override
          public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            _rebalanceRate.mark();
            AGGREGATED_REBALANCE_RATE.mark();
            //nop
            LOG.trace("Partition ownership assigned for {}.", partitions);
          }
        });

        while (!_shouldDie) {

          //read a batch of records
          try {
            records = consumer.poll(pollInterval);
            _numKafkaPolls.mark();
            _eventCountsPerPoll.update(records.count());
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
    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX_REGEX + EVENTS_PROCESSED_RATE));
    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX_REGEX + EVENTS_BYTE_PROCESSED_RATE));
    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX_REGEX + ERROR_RATE));

    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX_REGEX + REBALANCE_RATE));
    metrics.add(new BrooklinMeterInfo(METRICS_PREFIX_REGEX + NUM_KAFKA_POLLS));
    metrics.add(new BrooklinHistogramInfo(METRICS_PREFIX_REGEX + EVENT_COUNTS_PER_POLL));
    metrics.add(new BrooklinGaugeInfo(METRICS_PREFIX_REGEX + TIME_SINCE_LAST_EVENT_RECEIVED));
    metrics.add(new BrooklinGaugeInfo(METRICS_PREFIX_REGEX + TIME_SINCE_LAST_EVENT_PROCESSED));
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
        _errorRate.mark();
        AGGREGATED_ERROR_RATE.mark();
        // If flag _skipBadMessagesEnabled is set, then message are skipped after unsuccessfully
        // trying to send it "_retryCount" times. Example of problems:
        // - Message is above the message size supported by the destination.
        // - The message can not be encoded to conform to the destination format (e.g. missing a field).
        //
        // Unfortunately the error could be a transient network problem, and not a problem with the message itself.
        // For this reason is strongly recommended to put alerts in the _skippedBadMessagesRate and page the oncall
        // in case of errors.
        //
        // This flag should only be set to true for use cases that tolerate messages lost.
        // TODO: Try to define a special exception for "badMessage" so we can differenciate between a send error,
        // or a message compliance error. Right now is very hard to do that, because  will require to refactor a lot
        // of library and code we do not control.
        if (_skipBadMessagesEnabled) {
          LOG.error("Skipping Message. task: {} ;  error: {}", _taskName, e.toString());
          _skippedBadMessagesRate.mark();
          // Continue to next message
        } else {
          // Throw the exception and let the connector rewind and retry.
          throw e;
        }
      }
    }
  }

  private void sendMessage(ConsumerRecord<?, ?> record, long readTime, String strReadTime) throws InterruptedException {
    int count = 0;
    while (true) {
      count++;
      try {
        _lastEventReceivedTime = Instant.now();
        _eventsProcessedRate.mark();
        AGGREGATED_EVENTS_PROCESSED_RATE.mark();
        int numBytes = record.serializedKeySize() + record.serializedValueSize();
        _bytesProcessedRate.mark(numBytes);
        AGGREGATED_BYTES_PROCESSED_RATE.mark(numBytes);
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

  /**
   * Look for config {@value CFG_SKIP_BAD_MESSAGE} in the datastream metadata and returns its value.
   * Default value is false.
   */
  private static boolean skipBadMessageEnabled(DatastreamTask task) {
    return task.getDatastreams()
        .stream()
        .findFirst()
        .map(Datastream::getMetadata)
        .map(metadata -> metadata.getOrDefault(CFG_SKIP_BAD_MESSAGE, "false").toLowerCase().equals("true"))
        .orElse(false);
  }
}
