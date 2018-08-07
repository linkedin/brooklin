package com.linkedin.datastream.connectors.kafka;


import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.record.TimestampType;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.diag.DatastreamPositionResponse;
import com.linkedin.datastream.common.diag.PhysicalSourcePosition;
import com.linkedin.datastream.common.diag.PhysicalSources;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.kafka.KafkaDatastreamMetadataConstants;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskStatus;

/**
 * Base class for connector task, where the connector is Kafka-based. This base class provides basic structure for
 * Kafka-based connectors, which usually have similar processing pattern: subscribe() then run-loop to poll() and
 * process(). This also handles managing metrics as well as checkpointing at a configurable offset commit interval.
 */
abstract public class AbstractKafkaBasedConnectorTask implements Runnable, ConsumerRebalanceListener {

  protected final Logger _logger;

  private static final long COMMIT_RETRY_TIMEOUT_MS = 5000;
  private static final long COMMIT_RETRY_INTERVAL_MS = 1000;

  public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG_LATEST = "latest";
  public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG_EARLIEST = "earliest";

  protected long _lastCommittedTime = System.currentTimeMillis();
  protected int _eventsProcessedCount = 0;
  protected static final Duration LOG_EVENTS_PROCESSED_PROGRESS_DURATION = Duration.ofMinutes(1);
  protected Instant _eventsProcessedCountLoggedTime;
  private static final long POLL_BUFFER_TIME_MS = 1000;

  protected final Properties _consumerProps;
  protected final String _datastreamName;
  protected final Datastream _datastream;

  // lifecycle
  protected volatile boolean _shutdown = false;
  protected volatile long _lastPolledTimeMs = System.currentTimeMillis();
  protected final CountDownLatch _startedLatch = new CountDownLatch(1);
  protected final CountDownLatch _stoppedLatch = new CountDownLatch(1);

  // config
  protected DatastreamTask _datastreamTask;
  protected final long _offsetCommitInterval;
  protected final long _pollTimeoutMs;
  protected final Duration _retrySleepDuration;
  protected final int _maxRetryCount;
  protected final boolean _pausePartitionOnError;
  protected final Duration _pauseErrorPartitionDuration;
  protected final long _processingDelayLogThresholdMs;
  protected final Optional<Map<Integer, Long>> _startOffsets;
  protected final boolean _enableLatestBrokerOffsetsFetcher;

  protected volatile String _taskName;
  protected final DatastreamEventProducer _producer;
  protected Consumer<?, ?> _consumer;
  protected Set<TopicPartition> _consumerAssignment = Collections.emptySet();

  // Datastream task updates that need to be processed
  protected final Set<DatastreamConstants.UpdateType> _taskUpdates = Sets.newConcurrentHashSet();
  @VisibleForTesting
  int _pausedPartitionsConfigUpdateCount = 0;
  // paused partitions config contains the topic partitions that are configured for pause (via Datastream metadata)
  protected final Map<String, Set<String>> _pausedPartitionsConfig = new ConcurrentHashMap<>();
  // auto paused partitions map contains partitions that were paused on the fly (due to error or in-flight msg count)
  protected final Map<TopicPartition, PausedSourcePartitionMetadata> _autoPausedSourcePartitions = new ConcurrentHashMap<>();

  protected final KafkaBasedConnectorTaskMetrics _consumerMetrics;

  /**
   * A store of position data for each TopicPartition. This position data is what will be returned to this task's
   * connector. The position data will be in the form of event timestamp if timestamp data is available, and otherwise
   * will use Kafka offset.
   *
   * @see com.linkedin.datastream.common.diag.PhysicalSources
   *      which will map TopicPartition -> PhysicalSourcePosition
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition
   *      which will contain position data for both the broker and this consumer
   */
  private final PhysicalSources _positions = new PhysicalSources();

  /**
   * A store of position data for each TopicPartition. This position data will exclusively be Kafka offset based, and is
   * kept to assist in calculating if the current consumer is caught-up or not.
   *
   * @see com.linkedin.datastream.common.diag.PhysicalSources
   *      which will map TopicPartition -> PhysicalSourcePosition
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition
   *      which will contain position data for both the broker and this consumer
   */
  private final PhysicalSources _offsetPositions = new PhysicalSources();

  /**
   * Executor for a service which periodically queries Kafka via RPC for the latest available Kafka offsets.
   * @see #startLatestBrokerOffsetsFetcher()
   */
  private ExecutorService _latestBrokerOffsetsFetcher;

  protected AbstractKafkaBasedConnectorTask(KafkaBasedConnectorConfig config, DatastreamTask task, Logger logger,
      String metricsPrefix) {
    _logger = logger;
    _logger.info(
        "Creating Kafka-based connector task for datastream task {} with commit interval {} ms, retry sleep duration {}"
            + " ms, retry count {}, pausePartitionOnSendError {}", task, config.getCommitIntervalMillis(),
        config.getRetrySleepDuration().toMillis(), config.getRetryCount(), config.getPausePartitionOnError());
    _datastreamTask = task;
    _producer = task.getEventProducer();
    _datastream = task.getDatastreams().get(0);
    _datastreamName = _datastream.getName();
    _taskName = task.getDatastreamTaskName();

    _consumerProps = config.getConsumerProps();
    if (_datastream.getMetadata().containsKey(KafkaDatastreamMetadataConstants.CONSUMER_OFFSET_RESET_STRATEGY)) {
      String strategy = _datastream.getMetadata().get(KafkaDatastreamMetadataConstants.CONSUMER_OFFSET_RESET_STRATEGY);
      _logger.info("Datastream contains consumer config override for {} with value {}",
          ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, strategy);
      _consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, strategy);
    }

    _processingDelayLogThresholdMs = config.getProcessingDelayLogThresholdMs();
    _maxRetryCount = config.getRetryCount();
    _pausePartitionOnError = config.getPausePartitionOnError();
    _pauseErrorPartitionDuration = config.getPauseErrorPartitionDuration();
    _startOffsets = Optional.ofNullable(_datastream.getMetadata().get(DatastreamMetadataConstants.START_POSITION))
        .map(json -> JsonUtils.fromJson(json, new TypeReference<Map<Integer, Long>>() {
        }));
    _offsetCommitInterval = config.getCommitIntervalMillis();
    _pollTimeoutMs = config.getPollTimeoutMillis();
    _retrySleepDuration = config.getRetrySleepDuration();
    _enableLatestBrokerOffsetsFetcher = config.enableLatestBrokerOffsetsFetcher();
    _consumerMetrics = createKafkaBasedConnectorTaskMetrics(metricsPrefix, _datastreamName, _logger);
  }

  protected static String generateMetricsPrefix(String connectorName, String simpleClassName) {
    return StringUtils.isBlank(connectorName) ? simpleClassName : connectorName + "." + simpleClassName;
  }

  protected KafkaBasedConnectorTaskMetrics createKafkaBasedConnectorTaskMetrics(String metricsPrefix, String key,
      Logger errorLogger) {
    KafkaBasedConnectorTaskMetrics consumerMetrics =
        new KafkaBasedConnectorTaskMetrics(metricsPrefix, key, errorLogger);
    consumerMetrics.createEventProcessingMetrics();
    consumerMetrics.createPollMetrics();
    consumerMetrics.createPartitionMetrics();
    return consumerMetrics;
  }

  /**
   * Create a Kafka consumer using the consumer properties.
   * @param consumerProps the Kafka consumer properties
   * @return a KafkaConsumer
   */
  protected abstract Consumer<?, ?> createKafkaConsumer(Properties consumerProps);

  /**
   * Subscribe the consumer to a topic list or regex pattern and optionally set a callback (ConsumerRebalanceListener).
   */
  protected abstract void consumerSubscribe();

  /**
   * Translate the Kafka consumer record into a Datastream producer record.
   * @param fromKafka the Kafka consumer record
   * @param readTime the instant the record was polled from Kafka
   * @return a Datastream producer record
   */
  protected abstract DatastreamProducerRecord translate(ConsumerRecord<?, ?> fromKafka, Instant readTime)
      throws Exception;

  /**
   * Get the taskName
   */
  protected String getTaskName() {
    return _taskName;
  }


  /**
   * Translate the Kafka consumer records if necessary and send the batch of records to destination.
   * @param records the Kafka consumer records
   * @param readTime the instant the records were successfully polled from the Kafka source
   */
  protected void translateAndSendBatch(ConsumerRecords<?, ?> records, Instant readTime) {
    boolean shouldAddPausePartitionsTask = false;
    updateBrokerPositionMetadata(records, readTime);
    // iterate through each topic partition one at a time, for better isolation
    for (TopicPartition topicPartition : records.partitions()) {
      for (ConsumerRecord<?, ?> record : records.records(topicPartition)) {
        try {
          updatePositionMetadata(record, readTime);
          sendMessage(record, readTime);
        } catch (Exception e) {
          _consumerMetrics.updateErrorRate(1);
          // seek to previous checkpoints for this topic partition
          seekToLastCheckpoint(Collections.singleton(topicPartition));
          if (_pausePartitionOnError) {
            // if configured to pause partition on error conditions, add to auto-paused set
            _logger.warn("Got exception while sending record {}, adding topic partition {} to auto-pause set", record,
                topicPartition);
            Instant start = Instant.now();
            _autoPausedSourcePartitions.put(topicPartition,
                PausedSourcePartitionMetadata.sendError(start, _pauseErrorPartitionDuration));
            shouldAddPausePartitionsTask = true;
          }
          // skip other messages for this partition, but can continue processing other partitions
          break;
        }
      }
    }
    if (shouldAddPausePartitionsTask) {
      _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    }
  }

  /**
   * Updates the latest broker offsets based on internal Kafka consumer metrics. Since the consumer metrics are only
   * updated per topic partition actually fetched, we update only those positions.
   *
   * @param records the records fetched in the current poll
   * @param readTime the time the records were fetched
   */
  private void updateBrokerPositionMetadata(ConsumerRecords<?, ?> records, Instant readTime) {
    // Metric names per KIP-92, which applies to Kafka versions >= 0.10.2.0 and < 1.1.0
    BiPredicate<MetricName, TopicPartition> matchesKip92 =
        (metricName, topicPartition) -> metricName != null && metricName.name() != null && metricName.name()
            .equals(topicPartition + ".records-lag");
    // Metric names per KIP-225, which applies to Kafka versions >= 1.1.0
    BiPredicate<MetricName, TopicPartition> matchesKip225 =
        (metricName, topicPartition) -> metricName != null && metricName.name() != null && metricName.name()
            .equals("records-lag") && metricName.tags() != null && metricName.tags().containsKey("topic")
            && metricName.tags().get("topic").equals(topicPartition.topic()) && metricName.tags()
            .containsKey("partition") && metricName.tags()
            .get("partition")
            .equals(String.valueOf(topicPartition.partition()));

    for (TopicPartition topicPartition : records.partitions()) {
      for (MetricName metricName : _consumer.metrics().keySet()) {
        if (matchesKip92.or(matchesKip225).test(metricName, topicPartition)) {
          // Calculate what the broker position must be from the lag metric
          long consumerLag = (long) _consumer.metrics().get(metricName).value();
          long consumerPosition = _consumer.position(topicPartition);
          long brokerPosition = consumerPosition + consumerLag;

          // Build and update the broker offset position data
          PhysicalSourcePosition offsetPosition = new PhysicalSourcePosition();
          offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
          offsetPosition.setSourceQueriedTimeMs(readTime.toEpochMilli());
          offsetPosition.setSourcePosition(Long.toString(brokerPosition));
          _offsetPositions.update(topicPartition.toString(), offsetPosition);
        }
      }
    }
  }

  /**
   * Updates the current consumer position from the record we are currently reading.
   *
   * @param record the record we are currently reading
   * @param readTime the time the records were fetched
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition for information on what a position is
   */
  private void updatePositionMetadata(ConsumerRecord<?, ?> record, Instant readTime) {
    String physicalSource = new TopicPartition(record.topic(), record.partition()).toString();

    // Why do we add +1 to the record's offset? All the other Kafka APIs involved here from position() to endOffsets()
    // add +1. So, it is just a little more convenient to modify the position metadata we are storing here than it is
    // to handle the way all the other Kafka APIs return values.
    String offset = Long.toString(record.offset() + 1);

    // Update offset position. This is used to check for caught-up partitions when the response is actually returned.
    // See getPositionResponse() for details.
    PhysicalSourcePosition offsetPosition = new PhysicalSourcePosition();
    offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
    offsetPosition.setConsumerProcessedTimeMs(readTime.toEpochMilli());
    offsetPosition.setConsumerPosition(offset);
    _offsetPositions.update(physicalSource, offsetPosition);

    PhysicalSourcePosition position = new PhysicalSourcePosition();
    if (record.timestampType() != null && record.timestampType() == TimestampType.LOG_APPEND_TIME
        && record.timestamp() >= 0) {
      // If the event timestamp is available, let's use that.
      position.setPositionType(PhysicalSourcePosition.EVENT_TIME_POSTIION_TYPE);
      position.setSourceQueriedTimeMs(readTime.toEpochMilli());
      position.setSourcePosition(Long.toString(readTime.toEpochMilli()));
      position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
      position.setConsumerPosition(Long.toString(record.timestamp()));
    } else {
      // If the event timestamp isn't available, we'll use Kafka offset data instead.
      position.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
      position.setSourceQueriedTimeMs(_offsetPositions.get(physicalSource).getSourceQueriedTimeMs());
      position.setSourcePosition(_offsetPositions.get(physicalSource).getSourcePosition());
      position.setConsumerProcessedTimeMs(readTime.toEpochMilli());
      position.setConsumerPosition(offset);
    }
    _positions.update(physicalSource, position);
  }

  protected void sendMessage(ConsumerRecord<?, ?> record, Instant readTime) throws Exception {
    int sendAttempts = 0;
    DatastreamProducerRecord datastreamProducerRecord = translate(record, readTime);
    while (true) {
      try {
        sendAttempts++;
        sendDatastreamProducerRecord(datastreamProducerRecord);
        int numBytes = record.serializedKeySize() + record.serializedValueSize();
        _consumerMetrics.updateBytesProcessedRate(numBytes);
        return; // Break the retry loop and exit.
      } catch (Exception e) {
        _logger.error("Error sending Message. task: {} ; error: {};", _taskName, e.toString());
        _logger.error("Stack Trace: {}", Arrays.toString(e.getStackTrace()));
        if (_shutdown || sendAttempts >= _maxRetryCount) {
          _logger.error("Send messages failed with exception: {}", e);
          throw e;
        }
        _logger.warn("Sleeping for {} seconds before retrying. Retry {} of {}", _retrySleepDuration.getSeconds(),
            sendAttempts, _maxRetryCount);
        Thread.sleep(_retrySleepDuration.toMillis());
      }
    }
  }

  protected void sendDatastreamProducerRecord(DatastreamProducerRecord datastreamProducerRecord) throws Exception {
    _producer.send(datastreamProducerRecord, null);
  }

  @Override
  public void run() {
    _logger.info("Starting the Kafka-based connector task for {}", _datastreamTask);
    boolean startingUp = true;
    long pollInterval = 0; // so 1st call to poll is fast for purposes of startup

    _eventsProcessedCountLoggedTime = Instant.now();

    try {
      _consumer = createKafkaConsumer(_consumerProps);
      consumerSubscribe();

      if (_enableLatestBrokerOffsetsFetcher) {
        startLatestBrokerOffsetsFetcher();
      }

      ConsumerRecords<?, ?> records;
      while (!_shutdown) {
        // perform any pre-computations before poll()
        preConsumerPollHook();

        // read a batch of records
        records = pollRecords(pollInterval);

        // handle startup notification if this is the 1st poll call
        if (startingUp) {
          pollInterval = _pollTimeoutMs;
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
      _logger.info("Flushing and committing offsets before task {} exits.", _taskName);
      maybeCommitOffsets(_consumer, true);
    } catch (WakeupException e) {
      if (_shutdown) {
        _logger.info("Got WakeupException, shutting down task {}.", _taskName);
        maybeCommitOffsets(_consumer, true);
      } else {
        _logger.error("Got WakeupException while not in shutdown mode.", e);
        throw e;
      }
    } catch (Exception e) {
      _logger.error("{} failed with exception.", _taskName, e);
      _datastreamTask.setStatus(DatastreamTaskStatus.error(e.toString() + ExceptionUtils.getFullStackTrace(e)));
      throw new DatastreamRuntimeException(e);
    } finally {
      _stoppedLatch.countDown();
      if (null != _consumer) {
        _consumer.close();
      }
      _logger.info("{} stopped", _taskName);
    }
  }

  /**
   * Starts a service which uses an RPC to fetch the latest broker offset no more than once per minute, if that
   * information becomes out of date.
   *
   * As part of normal consumer operations, the latest broker offsets for a TopicPartition are fetched when records are
   * successfully retrieved for that topic partition. If that happens, there is no need to make a separate RPC call, as
   * we get those offsets from the Kafka consumer's metrics() API every time we get events
   * (see {@link #updateBrokerPositionMetadata(ConsumerRecords, Instant)}).
   *
   * However, if fetching records fails for whatever reason, then the stored information will become stale. The only way
   * for Brooklin to know for sure what the latest broker offsets are in this case is to make the RPC.
   */
  private void startLatestBrokerOffsetsFetcher() {
    _logger.info("Starting latest broker offsets fetcher for task {}", _taskName);
    _latestBrokerOffsetsFetcher = Executors.newFixedThreadPool(2,
        runnable -> new Thread(runnable, "latestBrokerOffsetsFetcher"));
    _latestBrokerOffsetsFetcher.submit(() -> {
      // We need to create a new consumer as we can't safely operate a Consumer from more than one thread.
      Consumer<?, ?> consumer = createKafkaConsumer(_consumerProps);
      while (!_shutdown) {
        Future<?> future = null;
        Instant currentTime = Instant.now();

        // We want to update only those partitions which are assigned to us and haven't been updated in over 1 minute.
        Set<TopicPartition> partitionsNeedingUpdate = _consumerAssignment.stream()
            .filter(tp -> Optional.ofNullable(_offsetPositions.get(tp.toString()))
                .map(PhysicalSourcePosition::getSourceQueriedTimeMs)
                .map(Instant::ofEpochMilli)
                .orElse(Instant.EPOCH)
                .isBefore(currentTime.minus(1, ChronoUnit.MINUTES)))
            .collect(Collectors.toSet());

        try {
          // Update the offsets, or timeout in 1 minute.
          future = _latestBrokerOffsetsFetcher.submit(updateLatestBrokerOffsetsByRpc(consumer, partitionsNeedingUpdate,
              currentTime.toEpochMilli()));
          future.get(1, TimeUnit.MINUTES);

          // Wait 1 minute before running this operation again.
          Thread.sleep(Duration.ofMinutes(1).toMillis());
        } catch (Exception e) {
          if (future != null) {
            future.cancel(true);
          }
          _logger.warn("Failed to update broker end offsets via RPC.", e);
        }
      }
      if (consumer != null) {
        consumer.close();
      }
    });
  }

  /**
   * Uses the specified consumer to make an RPC call to get the end offsets for the specified partitions and updates the
   * metadata. Use externally for testing purposes only. Note that the provided consumer must not be either the consumer
   * for this task or the consumer used within the endOffsetsUpdater task, or else a concurrent modification condition
   * may arise.
   *
   * @param consumer the specified consumer
   * @param partitions the partitions to fetch broker offsets for
   * @param currentTime the time the operation is asked for
   * @return a Runnable which will perform the specified operation
   */
  @VisibleForTesting
  Runnable updateLatestBrokerOffsetsByRpc(Consumer<?, ?> consumer, Set<TopicPartition> partitions, long currentTime) {
    return () -> consumer.endOffsets(partitions).forEach((tp, offset) -> {
      PhysicalSourcePosition offsetPosition = new PhysicalSourcePosition();
      offsetPosition.setPositionType(PhysicalSourcePosition.KAFKA_OFFSET_POSITION_TYPE);
      offsetPosition.setSourceQueriedTimeMs(currentTime);
      offsetPosition.setSourcePosition(String.valueOf(offset));
      _offsetPositions.update(tp.toString(), offsetPosition);
    });
  }

  public void stop() {
    _logger.info("{} stopping", _taskName);
    _shutdown = true;
    if (_latestBrokerOffsetsFetcher != null) {
      _latestBrokerOffsetsFetcher.shutdown();
      _latestBrokerOffsetsFetcher = null;
    }
    if (_consumer != null) {
      _logger.info("Waking up the consumer for task {}", _taskName);
      _consumer.wakeup();
    }
    _consumerMetrics.deregisterMetrics();
  }

  public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
    return _startedLatch.await(timeout, unit);
  }

  public boolean awaitStop(long timeout, TimeUnit unit) throws InterruptedException {
    return _stoppedLatch.await(timeout, unit);
  }

  protected void trackEventsProcessedProgress(int recordCount) {
    _eventsProcessedCount += recordCount;
    Duration timeSinceLastLog = Duration.between(_eventsProcessedCountLoggedTime, Instant.now());
    if (timeSinceLastLog.compareTo(LOG_EVENTS_PROCESSED_PROGRESS_DURATION) > 0) {
      _logger.info("Processed {} records in {} seconds for datastream {}", _eventsProcessedCount,
          timeSinceLastLog.getSeconds(), _datastreamName);
      _eventsProcessedCount = 0;
      _eventsProcessedCountLoggedTime = Instant.now();
    }
  }

  /**
   * Poll the records from Kafka using the specified timeout (milliseconds). If poll() fails and if retryCount is
   * configured, this method will sleep for some duration and try polling again.
   * @param pollInterval the timeout (in milliseconds) spent waiting in poll() if data is not available in buffer.
   * @return the Kafka consumer records polled
   * @throws InterruptedException if thread is interrupted during retry sleep
   */
  protected ConsumerRecords<?, ?> pollRecords(long pollInterval) throws InterruptedException {
    ConsumerRecords<?, ?> records;
    try {
      long curPollTime = System.currentTimeMillis();
      _lastPolledTimeMs = curPollTime;
      records = _consumer.poll(pollInterval);
      long pollDurationMs = System.currentTimeMillis() - curPollTime;
      if (pollDurationMs > pollInterval + POLL_BUFFER_TIME_MS) {
        // record poll time exceeding client poll timeout
        _logger.warn("ConsumerId: {}, Kafka client poll took {} ms (> poll timeout {} + buffer time {} ms)", _taskName,
            pollDurationMs, pollInterval, POLL_BUFFER_TIME_MS);
        _consumerMetrics.updateClientPollOverTimeout(1);
      }
      _consumerMetrics.updateNumPolls(1);
      _consumerMetrics.updateEventCountsPerPoll(records.count());
      if (!records.isEmpty()) {
        _consumerMetrics.updateEventsProcessedRate(records.count());
        _consumerMetrics.updateLastEventReceivedTime(Instant.now());
      }
      return records;
    } catch (NoOffsetForPartitionException e) {
      handleNoOffsetForPartitionException(e);
      return ConsumerRecords.EMPTY;
    } catch (WakeupException e) {
      _logger.warn("Got a WakeupException, shutdown in progress.", e);
      return ConsumerRecords.EMPTY;
    } catch (Exception e) {
      handlePollRecordsException(e);
      return ConsumerRecords.EMPTY;
    }
  }

  protected long getLastPolledTimeMs() {
    return _lastPolledTimeMs;
  }

  /**
   * Processes the Kafka consumer records by translating them and sending them to the event producer.
   * @param records the Kafka consumer records
   * @param readTime the time at which the records were successfully polled from Kafka
   */
  protected void processRecords(ConsumerRecords<?, ?> records, Instant readTime) {
    // send the batch out the other end
    // TODO(misanchez): we should have a way to signal the producer to stop and throw an exception
    //                  in case the _shutdown signal is set (similar to kafka wakeup)
    translateAndSendBatch(records, readTime);

    if (System.currentTimeMillis() - readTime.toEpochMilli() > _processingDelayLogThresholdMs) {
      _consumerMetrics.updateProcessingAboveThreshold(1);
    }

    if (!_shutdown) {
      // potentially commit our offsets (if its been long enough and all sends were successful)
      maybeCommitOffsets(_consumer, false);
    }
  }

  /**
   * Handle when Kafka consumer throws OffsetOutOfRangeException. The base behavior is no-op.
   * @param e the Exception
   */
  protected void handleOffsetOutOfRangeException(OffsetOutOfRangeException e) {
  }

  /**
   * Handle when Kafka consumer throws NoOffsetForPartitionException. The base behavior is to seek to start position
   * for all partitions.
   * @param e the Exception
   */
  protected void handleNoOffsetForPartitionException(NoOffsetForPartitionException e) {
    _logger.info("Poll threw NoOffsetForPartitionException for partitions {}.", e.partitions());
    if (!_shutdown) {
      seekToStartPosition(_consumer, e.partitions());
    }
  }

  /**
   * Handle exception during pollRecords(). The base behavior is to sleep for some time.
   * @param e the Exception
   * @throws InterruptedException
   */
  protected void handlePollRecordsException(Exception e) throws InterruptedException {
    _logger.warn("Poll threw an exception. Sleeping for {} seconds and retrying. Exception: {}",
        _retrySleepDuration.getSeconds(), e);
    if (!_shutdown) {
      Thread.sleep(_retrySleepDuration.toMillis());
    }
  }

  /**
   * Flush the producer and commit the offsets if the configured offset commit interval has been reached, or if
   * force is set to true.
   * @param consumer the Kafka consumer
   * @param force whether flush and commit should happen unconditionally
   */
  protected void maybeCommitOffsets(Consumer<?, ?> consumer, boolean force) {
    long now = System.currentTimeMillis();
    long timeSinceLastCommit = now - _lastCommittedTime;
    if (force || timeSinceLastCommit > _offsetCommitInterval) {
      _logger.info("Trying to flush the producer and commit offsets.");
      _producer.flush();
      try {
        commitWithRetries(consumer, Optional.empty());
        _lastCommittedTime = System.currentTimeMillis();
      } catch (DatastreamRuntimeException e) {
        if (force) { // if forced commit then throw exception
          _logger.error("Forced commit failed after retrying, so exiting.", e);
          throw e;
        } else {
          _logger.warn("Commit failed after several retries.", e);
        }
      }
    }
  }

  protected void commitWithRetries(Consumer<?, ?> consumer, Optional<Map<TopicPartition, OffsetAndMetadata>> offsets)
      throws DatastreamRuntimeException {
    boolean result = PollUtils.poll(() -> {
      try {
        if (offsets.isPresent()) {
          consumer.commitSync(offsets.get());
        } else {
          consumer.commitSync();
        }
        _logger.info("Commit succeeded.");
      } catch (KafkaException e) {
        if (_shutdown) {
          _logger.info("Caught KafkaException in commitWithRetries while shutting down, so exiting. Exception={}", e);
          return true;
        }
        _logger.warn("Commit failed with exception. DatastreamTask = {}", _datastreamTask.getDatastreamTaskName(), e);
        return false;
      }

      return true;
    }, COMMIT_RETRY_INTERVAL_MS, COMMIT_RETRY_TIMEOUT_MS);

    if (!result) {
      String msg = "Commit failed after several retries, Giving up.";
      _logger.error(msg);
      throw new DatastreamRuntimeException(msg);
    }
  }

  /**
   * Seek to the last checkpoint for the given topicPartitions.
   */
  private void seekToLastCheckpoint(Set<TopicPartition> topicPartitions) {
    _logger.info("Trying to seek to previous checkpoint for partitions: {}", topicPartitions);
    Map<TopicPartition, OffsetAndMetadata> lastCheckpoint = new HashMap<>();
    Set<TopicPartition> tpWithNoCommits = new HashSet<>();
    // construct last checkpoint
    topicPartitions.forEach(tp -> {
      OffsetAndMetadata offset = _consumer.committed(tp);
      // offset can be null if there was no prior commit
      if (offset == null) {
        tpWithNoCommits.add(tp);
      } else {
        lastCheckpoint.put(tp, offset);
      }
    });
    _logger.info("Seeking to previous checkpoints {}", lastCheckpoint);
    // reset consumer to last checkpoint
    lastCheckpoint.forEach((tp, offsetAndMetadata) -> _consumer.seek(tp, offsetAndMetadata.offset()));
    if (!tpWithNoCommits.isEmpty()) {
      _logger.info("Seeking to start position for partitions: {}", tpWithNoCommits);
      seekToStartPosition(_consumer, tpWithNoCommits);
    }
  }

  private void seekToStartPosition(Consumer<?, ?> consumer, Set<TopicPartition> partitions) {
    if (_startOffsets.isPresent()) {
      _logger.info("Datastream is configured with StartPosition. Trying to start from {}", _startOffsets.get());
      seekToOffset(consumer, partitions, _startOffsets.get());
    } else {
      // means we have no saved offsets for some partitions, seek to end or beginning based on consumer config
      if (CONSUMER_AUTO_OFFSET_RESET_CONFIG_LATEST.equals(
          _consumerProps.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
              CONSUMER_AUTO_OFFSET_RESET_CONFIG_LATEST))) {
        _logger.info("Datastream was not configured with StartPosition. Seeking to end for partitions: {}", partitions);
        consumer.seekToEnd(partitions);
      } else {
        _logger.info("Datastream was not configured with StartPosition. Seeking to beginning for partitions: {}",
            partitions);
        consumer.seekToBeginning(partitions);
      }
    }
  }

  private void seekToOffset(Consumer<?, ?> consumer, Set<TopicPartition> partitions, Map<Integer, Long> startOffsets) {
    partitions.forEach(tp -> {
      Long offset = startOffsets.get(tp.partition());
      if (offset == null) {
        String msg = String.format("Couldn't find the offset for partition %s", tp);
        _logger.error(msg);
        throw new DatastreamRuntimeException(msg);
      }
      consumer.seek(tp, offset);
    });

    _logger.info("Seek completed to the offsets.");
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
    _logger.info("Partition ownership revoked for {}, checkpointing.", topicPartitions);
    if (!_shutdown && !topicPartitions.isEmpty()) { // there is a commit at the end of the run method, skip extra commit in shouldDie mode.
      try {
        maybeCommitOffsets(_consumer, true); // happens inline as part of poll
      } catch (Exception e) {
        // log the exception and let the new partition owner just read from previous checkpoint
        _logger.warn("Caught exception while trying to commit offsets in onPartitionsRevoked with partitions {}.",
            topicPartitions, e);
      }
    }

    _consumerAssignment = Sets.newHashSet(_consumer.assignment());
    _logger.info("Current assignment is {}", _consumerAssignment);

    // Remove old position data
    _positions.retainAll(_consumerAssignment.stream().map(TopicPartition::toString).collect(Collectors.toSet()));
    _offsetPositions.retainAll(_consumerAssignment.stream().map(TopicPartition::toString).collect(Collectors.toSet()));

    // update paused partitions
    _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    _consumerMetrics.updateRebalanceRate(1);
    _logger.info("Partition ownership assigned for {}.", partitions);

    _consumerAssignment = Sets.newHashSet(partitions);
    _logger.info("Current assignment is {}", _consumerAssignment);

    // update paused partitions, in case.
    _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
  }

  /**
   * This method is called before making a call to poll() for any precomputations needed.
   *
   * Note: This method is supposed to be called from run() thread, so in effect, it is "single threaded".
   * The only updates that can happen to _taskUpdates queue outside this method is addition of
   * new update type when there is any update to datastream task (in method checkForUpdateTask())
   */
  protected void preConsumerPollHook() {
    // check if any auto-paused partitions need to be resumed
    checkForPartitionsToAutoResume();

    // check if there was any update in task and take actions
    for (DatastreamConstants.UpdateType updateType : _taskUpdates) {
      _taskUpdates.remove(updateType);
      if (updateType != null) {
        switch (updateType) {
          case PAUSE_RESUME_PARTITIONS:
            pausePartitions();
            break;
          default:
            String msg = String.format("Unknown update type %s for task %s.", updateType, _taskName);
            _logger.error(msg);
            throw new DatastreamRuntimeException(msg);
        }
      } else {
        throw new IllegalStateException("Found null update type in task updates set.");
      }
    }
  }

  /**
   * The method, given a datastream task - checks if there is any update in the existing task.
   * @param datastreamTask - datastream task to compare with.
   */
  @VisibleForTesting
  public void checkForUpdateTask(DatastreamTask datastreamTask) {
    // check if there was any change in paused partitions.
    checkForPausedPartitionsUpdate(datastreamTask);
  }

  /**
   * Checks if there is any change in set of paused partitions for DatastreamTask.
   * @param datastreamTask the Datastream task
   */
  private void checkForPausedPartitionsUpdate(DatastreamTask datastreamTask) {
    // check if there is any change in paused partitions
    // first get the given set of paused source partitions from metadata
    Datastream datastream = datastreamTask.getDatastreams().get(0);
    Map<String, Set<String>> newPausedSourcePartitionsMap = DatastreamUtils.getDatastreamSourcePartitions(datastream);

    if (!newPausedSourcePartitionsMap.equals(_pausedPartitionsConfig)) {
      _logger.info(
          "Difference in partitions found in Datastream metadata paused partitions for datastream {}. The list is: {}. "
              + "Adding {} to taskUpdates queue", datastream, newPausedSourcePartitionsMap,
          DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
      _pausedPartitionsConfig.clear();
      _pausedPartitionsConfig.putAll(newPausedSourcePartitionsMap);
      _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    }
  }

  /**
   * Checks for partitions to auto-resume by checking if any of the auto-paused partitions meets criteria to resume. If
   * such partition is found, add the PAUSE_RESUME_PARTITIONS task to the taskUpdates set.
   */
  private void checkForPartitionsToAutoResume() {
    if (_autoPausedSourcePartitions.values().stream().anyMatch(PausedSourcePartitionMetadata::shouldResume)) {
      _logger.info("Found partition to resume, adding PAUSE_RESUME_PARTITIONS to task updates set");
      _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    }
  }

  private void pausePartitions() {
    _logger.info("Checking for partitions to pause or resume.");
    Validate.isTrue(_consumer != null, "Consumer cannot be null when pausing partitions.");

    _pausedPartitionsConfigUpdateCount++; // increment counter for testing purposes only

    // get the config that's already there on the consumer
    Set<TopicPartition> currentPausedPartitions = _consumer.paused();
    Set<TopicPartition> currentAssignedPartitions = _consumer.assignment();

    // print state
    _logger.info("Current partition assignment for task {} is: {}, and paused partitions are: {}", _taskName,
        currentAssignedPartitions, currentPausedPartitions);
    _logger.info("Current auto-pause partition set is: {}", _autoPausedSourcePartitions);

    // Get the set of partitions to pause
    Set<TopicPartition> partitionsToPause =
        determinePartitionsToPause(currentAssignedPartitions, _pausedPartitionsConfig, _autoPausedSourcePartitions,
            _logger);

    // keep the auto-paused set up to date with only assigned partitions
    _autoPausedSourcePartitions.keySet().retainAll(currentAssignedPartitions);

    // need to pause the entire new set of partitions to pause, since they were resumed at the beginning
    _logger.info("Full pause list is: {}", partitionsToPause);

    if (!partitionsToPause.equals(currentPausedPartitions)) {
      // resume all paused partitions
      _consumer.resume(currentPausedPartitions);

      // only log the new list if there was a change
      _logger.info("There were new partitions to pause. New partitions pause list is: {}", partitionsToPause);
      _consumer.pause(partitionsToPause);

      // update metric
      // Get #of auto paused partitions on error first
      long numAutoPausedPartitionsOnError = _autoPausedSourcePartitions.values()
          .stream()
          .filter(metadata -> metadata.getReason().equals(PausedSourcePartitionMetadata.Reason.SEND_ERROR))
          .count();
      _consumerMetrics.updateNumAutoPausedPartitionsOnError(numAutoPausedPartitionsOnError);
      _consumerMetrics.updateNumAutoPausedPartitionsOnInFlightMessages(
          _autoPausedSourcePartitions.size() - numAutoPausedPartitionsOnError);
      _consumerMetrics.updateNumConfigPausedPartitions(partitionsToPause.size() - _autoPausedSourcePartitions.size());
    }
  }

  /**
   * Determine which partitions to pause. A partition should be paused if: it is configured for pause (via Datastream
   * metadata) or it is set for auto-pause (because of error or high-throughput).
   *
   * WARNING: this method has a side-effect of removing from the auto-pause set any partition that exists in the map
   * of partitions that are configured for pause or any partition that should be resumed
   * @param currentAssignment the set of topic partitions assigned to the consumer
   * @param configuredForPause a map of topic to partitions that are configured for pause
   * @param autoPausedSourcePartitions topic partitions that are auto-paused
   * @param logger the logger
   * @return the total set of assigned partitions that need to be paused
   */
  protected static Set<TopicPartition> determinePartitionsToPause(Set<TopicPartition> currentAssignment,
      Map<String, Set<String>> configuredForPause,
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedSourcePartitions, Logger logger) {
    Set<TopicPartition> partitionsToPause = new HashSet<>();
    for (TopicPartition assigned : currentAssignment) {
      // if configured for pause, add to set
      if (isPartitionConfiguredForPause(assigned, configuredForPause)) {
        partitionsToPause.add(assigned);
      }

      // if partition should be auto-paused, add to set
      PausedSourcePartitionMetadata metadata = autoPausedSourcePartitions.get(assigned);
      if (metadata != null) {
        if (metadata.shouldResume()) {
          // criteria to resume is met, remove the partition from auto-paused collection
          logger.info("Removing partition {} from auto-paused set, as criteria to resume as been met", assigned);
          autoPausedSourcePartitions.remove(assigned);
        } else {
          // criteria to resume is not yet met, add the auto-paused partition to the set of partitions to pause
          boolean uniquePartition = partitionsToPause.add(assigned);
          if (!uniquePartition) {
            // partition was already added by configuration, so remove from auto-paused collection
            logger.info(
                "Removing partition {} from auto-paused set, as partition is configured for pause in Datastream metadata",
                assigned);
            autoPausedSourcePartitions.remove(assigned);
          }
        }
      }
    }
    return partitionsToPause;
  }

  public KafkaDatastreamStatesResponse getKafkaDatastreamStatesResponse() {
    return new KafkaDatastreamStatesResponse(_datastreamName, _autoPausedSourcePartitions, _pausedPartitionsConfig,
        _consumerAssignment);
  }

  /**
   * If the set of paused partitions in the config contains all topics (denoted by REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC)
   * or contains the given partition explicitly, then the partition is configured to be paused.
   * @param topicPartition the topic partition
   * @param pauseConfiguration config map of topic to set of partitions to pause
   * @return true if the topic partition is configured to be paused.
   */
  private static boolean isPartitionConfiguredForPause(TopicPartition topicPartition,
      Map<String, Set<String>> pauseConfiguration) {
    Set<String> pausedPartitionsForTopic = pauseConfiguration.get(topicPartition.topic());
    return pausedPartitionsForTopic != null && (
        pausedPartitionsForTopic.contains(DatastreamMetadataConstants.REGEX_PAUSE_ALL_PARTITIONS_IN_A_TOPIC)
            || pausedPartitionsForTopic.contains(Integer.toString(topicPartition.partition())));
  }

  public static List<BrooklinMetricInfo> getMetricInfos(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(KafkaBasedConnectorTaskMetrics.getEventProcessingMetrics(prefix));
    metrics.addAll(KafkaBasedConnectorTaskMetrics.getEventPollMetrics(prefix));
    metrics.addAll(KafkaBasedConnectorTaskMetrics.getPartitionSpecificMetrics(prefix));
    metrics.addAll(KafkaBasedConnectorTaskMetrics.getKafkaBasedConnectorTaskSpecificMetrics(prefix));
    return metrics;
  }

  @VisibleForTesting
  public int getPausedPartitionsConfigUpdateCount() {
    return _pausedPartitionsConfigUpdateCount;
  }

  @VisibleForTesting
  public Set<TopicPartition> getAutoPausedSourcePartitions() {
    return Collections.unmodifiableSet(_autoPausedSourcePartitions.keySet());
  }

  @VisibleForTesting
  public Map<String, Set<String>> getPausedPartitionsConfig() {
    return Collections.unmodifiableMap(_pausedPartitionsConfig);
  }

  public boolean hasDatastream(String datastreamName) {
    return _datastreamName.equals(datastreamName);
  }

  @VisibleForTesting
  public static String getTaskMetadataGroupId(DatastreamTask task, CommonConnectorMetrics consumerMetrics,
      Logger logger) {
    Set<String> groupIds = DatastreamUtils.getMetadataGroupIDs(task.getDatastreams());
    if (!groupIds.isEmpty()) {
      if (groupIds.size() != 1) {
        String errMsg =
            String.format("Found multiple consumer group ids for connector task: %s. Group IDs: %s. Datastreams: %s",
                task.getId(), groupIds, task.getDatastreams());
        consumerMetrics.updateErrorRate(1, errMsg, null);
        throw new DatastreamRuntimeException(errMsg);
      }
      // if group ID is present in metadata, add it to properties even if present already.
      logger.info("Found overridden group ID for Kafka datastream task: {} . Overridden group id: {} Datastreams: %s",
          task.getId(), groupIds.toArray()[0], task.getDatastreams());
      return (String) groupIds.toArray()[0];
    }

    return null;
  }

  /**
   * Gets a DatastreamPositionResponse containing position data for the current task.
   * @return the current position data
   * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition for information on what a position is
   */
  public DatastreamPositionResponse getPositionResponse() {
    // Create the response
    DatastreamPositionResponse response = new DatastreamPositionResponse();

    // Update the position data for any caught-up partitions
    _offsetPositions.getPhysicalSourceToPosition().forEach((tp, offsetPosition) -> {
      long consumerPosition = Long.parseLong(offsetPosition.getConsumerPosition()); // Our consumer's offset
      long sourcePosition = Long.parseLong(offsetPosition.getSourcePosition()); // The broker's last offset
      long consumerProcessedTimeMs = offsetPosition.getConsumerProcessedTimeMs(); // The last time we processed an event
      long sourceQueriedTimeMs = offsetPosition.getSourceQueriedTimeMs(); // The last time we fetched the broker's last offset data

      if (sourceQueriedTimeMs > consumerProcessedTimeMs) {
        if (sourcePosition == consumerPosition) {
          // We are caught-up -- our consumer position matches the broker position.
          PhysicalSourcePosition position = _positions.get(tp);

          // We want to update our current position to describe this caught up state since our consumer position data is
          // more stale than the broker position data we have. How do we do this?
          //
          // We imagine there is an imaginary 'heartbeat' event which doesn't have an offset (since it's imaginary)
          // that's positioned at the last time we successfully fetched broker offsets, and we update the metadata
          // accordingly.

          // If we are using event time positions, then we should update our event time position.
          if (position.getPositionType().equals(PhysicalSourcePosition.EVENT_TIME_POSTIION_TYPE)) {
            position.setConsumerPosition(Long.toString(sourceQueriedTimeMs));
          }
          // If we aren't using event times (we are using offsets), then we shouldn't modify the value.

          // We update our last processed time to the last time we fetched the broker's position data.
          position.setConsumerProcessedTimeMs(sourceQueriedTimeMs);
        }
      }
    });

    // Build the map of datastreams -> physical sources
    Map<String, PhysicalSources> datastreamsToPhysicalSources = _datastreamTask.getDatastreams()
        .stream()
        .map(Datastream::getName)
        .collect(Collectors.toMap(name -> name, name -> _positions));

    // Put it into the response
    response.setDatastreamToPhysicalSources(datastreamsToPhysicalSources);

    return response;
  }
}
