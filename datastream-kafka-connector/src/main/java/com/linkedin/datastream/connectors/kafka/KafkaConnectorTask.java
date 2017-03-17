package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringJoiner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SlidingTimeWindowReservoir;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
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
import com.linkedin.datastream.server.Pair;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;


public class KafkaConnectorTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectorTask.class);
  private static final String CLASS_NAME = KafkaConnectorTask.class.getSimpleName();

  private final KafkaConsumerFactory<?, ?> _consumerFactory;
  private final Properties _consumerProps;
  private final DynamicMetricsManager _dynamicMetricsManager;
  private final String _datastreamName;

  //lifecycle
  private volatile boolean _shouldDie = false;
  private final CountDownLatch _startedLatch = new CountDownLatch(1);
  private final CountDownLatch _stoppedLatch = new CountDownLatch(1);
  //config
  private final DatastreamTask _task;
  private final long _offsetCommitInterval;
  //state
  private volatile Thread _thread;
  private volatile String _taskName;
  private String _srcValue;
  private DatastreamEventProducer _producer;

  private volatile ProgressTracker _progressTracker;

  private static final String EVENTS_PROCESSED_RATE = "eventsProcessedRate";
  private static final String EVENTS_BYTE_PROCESSED_RATE = "eventsByteProcessedRate";
  private static final String AGGREGATE = "aggregate";
  private static final String ERROR_RATE = "errorRate";
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

  public KafkaConnectorTask(KafkaConsumerFactory<?, ?> factory, Properties consumerProps, DatastreamTask task,
      long commitIntervalMillis) {
    LOG.info("Creating kafka connector task for datastream task {} with commit interval {}Ms", task,
        commitIntervalMillis);
    _consumerProps = consumerProps;
    _consumerFactory = factory;
    _task = task;
    _offsetCommitInterval = commitIntervalMillis;
    _datastreamName = task.getDatastreams().get(0).getName();

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();

    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, EVENTS_PROCESSED_RATE, _eventsProcessedRate);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, EVENTS_BYTE_PROCESSED_RATE, _bytesProcessedRate);
    _dynamicMetricsManager.registerMetric(CLASS_NAME, _datastreamName, ERROR_RATE, _errorRate);

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

  @Override
  public void run() {
    LOG.info("Starting the kafka connector task for {}", _task);
    boolean startingUp = true;
    long pollInterval = 0; //so 1st call to poll is fast for purposes of startup
    _thread = Thread.currentThread();
    try {

      DatastreamSource source = _task.getDatastreamSource();
      KafkaConnectionString srcConnString = KafkaConnectionString.valueOf(source.getConnectionString());
      StringJoiner csv = new StringJoiner(",");
      srcConnString.getBrokers().forEach(broker -> csv.add(broker.toString()));
      String bootstrapValue = csv.toString();
      _srcValue = srcConnString.toString();

      DatastreamDestination destination = _task.getDatastreamDestination();
      String dstConnString = destination.getConnectionString();
      _producer = _task.getEventProducer();
      _taskName = srcConnString + "-to-" + dstConnString;

      Properties props = new Properties();
      props.put("bootstrap.servers", bootstrapValue);
      props.put("group.id", _taskName);
      props.put("enable.auto.commit", "false"); //auto-commits are unsafe
      props.put("auto.offset.reset", "none");
      props.putAll(_consumerProps);

      try (Consumer<?, ?> consumer = _consumerFactory.createConsumer(props)) {

        ConsumerRecords<?, ?> records;
        _progressTracker = new ProgressTracker();

        consumer.subscribe(Collections.singletonList(srcConnString.getTopicName()), new ConsumerRebalanceListener() {
          @Override
          public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            LOG.trace("Partition ownership revoked for {}, checkpointing.", partitions);
            maybeCommitOffsets(consumer, true); //happens inline as part of poll
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
            //means we have no saved offsets for some partitions, reset it to latest for those
            consumer.seekToEnd(e.partitions());
            continue;
          }

          //handle startup notification if this is the 1st poll call
          if (startingUp) {
            pollInterval = _offsetCommitInterval / 2; //leave time for processing, assume 50-50
            startingUp = false;
            _startedLatch.countDown();
          }

          //send the batch out the other end
          long readTime = System.currentTimeMillis();
          String strReadTime = String.valueOf(readTime);
          translateAndSendBatch(records, readTime, strReadTime, _progressTracker);

          //potentially commit our offsets (if its been long enough and all sends were successful)
          maybeCommitOffsets(consumer, false);
        }

        //shutdown
        maybeCommitOffsets(consumer, true);
      }
    } catch (Exception e) {
      LOG.error("{} failed with exception.", _taskName, e);
      _task.setStatus(DatastreamTaskStatus.error(e.getMessage()));
      throw e;
    } finally {
      _stoppedLatch.countDown();
      LOG.info("{} stopped", _taskName);
    }
  }

  public void stop() {
    LOG.info("{} stopping", _taskName);
    _shouldDie = true;
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
    ProgressTracker progressTracker = _progressTracker;
    boolean anyWorkDone = progressTracker.anyWorkDone();
    if (!anyWorkDone) {
      return;
    }
    long now = System.currentTimeMillis();
    long timeSinceLastCommit = now - progressTracker.since;
    if (force || timeSinceLastCommit > _offsetCommitInterval) {
      progressTracker.awaitCompletion(); //wait until all sends of the current batch succeed or anything fails
      Pair<DatastreamRecordMetadata, Exception> failure = progressTracker.getFirstFailure();
      if (failure != null) {
        Map<TopicPartition, OffsetAndMetadata> lastCheckpoint = new HashMap<>();
        //construct last checkpoint
        consumer.assignment().forEach(topicPartition -> {
          lastCheckpoint.put(topicPartition, consumer.committed(topicPartition));
        });
        //reset consumer to last checkpoint
        lastCheckpoint.forEach((topicPartition, offsetAndMetadata) -> {
          consumer.seek(topicPartition, offsetAndMetadata.offset());
        });
      } else {
        _producer.flush();
        consumer.commitSync();
      }
      _progressTracker = new ProgressTracker();
    }
  }

  private void translateAndSendBatch(ConsumerRecords<?, ?> records, long readTime, String strReadTime,
      ProgressTracker progressTracker) {
    try {
      progressTracker.markToBeSent(records.count());
      records.forEach(record -> {
        _lastEventReceivedTime = Instant.now();
        _eventsProcessedRate.mark();
        AGGREGATED_EVENTS_PROCESSED_RATE.mark();
        int numBytes = record.serializedKeySize() + record.serializedValueSize();
        _bytesProcessedRate.mark(numBytes);
        AGGREGATED_BYTES_PROCESSED_RATE.mark(numBytes);
        _producer.send(translate(record, readTime, strReadTime), progressTracker);
      });
    } catch (Exception e) {
      _errorRate.mark();
      AGGREGATED_ERROR_RATE.mark();
      //some part of the batch failed to send out, or some other exception
      //progressTracker will only record the 1st failure, so we dont need to care
      //about this overriding any previous failure captured by a callback
      progressTracker.markFailed(null, e);
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

  private class ProgressTracker implements SendCallback {
    private final long since = System.currentTimeMillis();
    private final AtomicInteger msgsToBeSent = new AtomicInteger(0);
    private final AtomicInteger msgsSucceeded = new AtomicInteger(0);
    private final CountDownLatch doneLatch = new CountDownLatch(1);
    private final AtomicReference<Boolean> success = new AtomicReference<>(null);
    private volatile Pair<DatastreamRecordMetadata, Exception> firstFailure;

    @Override
    public void onCompletion(DatastreamRecordMetadata metadata, Exception exception) {
      if (exception == null) {
        _lastEventProcessedTime = Instant.now();
        markSucceeded();
      } else {
        markFailed(metadata, exception);
      }
    }

    private void markToBeSent(int count) {
      msgsToBeSent.addAndGet(count);
    }

    private void markSucceeded() {
      if (msgsSucceeded.incrementAndGet() == msgsToBeSent.get()) {
        if (success.compareAndSet(null, Boolean.TRUE)) {
          doneLatch.countDown(); //successful completion
        }
      }
    }

    private void markFailed(DatastreamRecordMetadata metadata, Exception exception) {
      if (success.compareAndSet(null, Boolean.FALSE)) {
        //first failure
        firstFailure = new Pair<>(metadata, exception);
        doneLatch.countDown();
        LOG.error("sending event failed. metadata: {}", metadata, exception);
      } else {
        if (!Boolean.FALSE.equals(success.get())) {
          throw new IllegalStateException("should not be possible");
        }
      }
    }

    private boolean anyWorkDone() {
      return msgsToBeSent.get() > 0;
    }

    private void awaitCompletion() {
      while (true) {
        try {
          doneLatch.await();
          break;
        } catch (InterruptedException e) {
          //ignore on purpose and keep waiting
        }
      }
    }

    public Pair<DatastreamRecordMetadata, Exception> getFirstFailure() {
      return firstFailure;
    }
  }
}
