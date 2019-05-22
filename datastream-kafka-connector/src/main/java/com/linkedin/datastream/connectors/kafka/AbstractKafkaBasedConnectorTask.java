/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.Validate;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.InvalidOffsetException;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamConstants;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
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

  private static final long COMMIT_RETRY_TIMEOUT_MILLIS = 5000;
  private static final long COMMIT_RETRY_INTERVAL_MILLIS = 1000;

  public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG_LATEST = "latest";
  public static final String CONSUMER_AUTO_OFFSET_RESET_CONFIG_EARLIEST = "earliest";

  protected long _lastCommittedTime = System.currentTimeMillis();
  protected int _eventsProcessedCount = 0;
  protected static final Duration LOG_EVENTS_PROCESSED_PROGRESS_DURATION = Duration.ofMinutes(1);
  protected Instant _eventsProcessedCountLoggedTime;
  private static final long POLL_BUFFER_TIME_MILLIS = 1000;

  protected final Properties _consumerProps;
  protected final String _datastreamName;
  protected final Datastream _datastream;

  // lifecycle
  private volatile Thread _connectorTaskThread;
  protected volatile boolean _shutdown = false;
  protected volatile long _lastPolledTimeMillis = System.currentTimeMillis();
  protected final CountDownLatch _startedLatch = new CountDownLatch(1);
  protected final CountDownLatch _stoppedLatch = new CountDownLatch(1);

  // config
  protected DatastreamTask _datastreamTask;
  protected final long _offsetCommitInterval;
  protected final Duration _commitTimeout;
  protected final long _pollTimeoutMillis;
  protected final Duration _retrySleepDuration;
  protected final int _maxRetryCount;
  protected final boolean _pausePartitionOnError;
  protected final Duration _pauseErrorPartitionDuration;
  protected final long _processingDelayLogThresholdMillis;
  protected final Optional<Map<Integer, Long>> _startOffsets;

  protected volatile String _taskName;
  protected final DatastreamEventProducer _producer;
  protected Consumer<?, ?> _consumer;
  protected final Optional<KafkaPositionTracker> _kafkaPositionTracker;
  protected final Set<TopicPartition> _consumerAssignment = new HashSet<>();

  // Datastream task updates that need to be processed
  protected final Set<DatastreamConstants.UpdateType> _taskUpdates = Sets.newConcurrentHashSet();
  @VisibleForTesting
  int _pausedPartitionsConfigUpdateCount = 0;
  // paused partitions config contains the topic partitions that are configured for pause (via Datastream metadata)
  protected final Map<String, Set<String>> _pausedPartitionsConfig = new ConcurrentHashMap<>();
  // auto paused partitions map contains partitions that were paused on the fly (due to error or in-flight msg count)
  protected final Map<TopicPartition, PausedSourcePartitionMetadata> _autoPausedSourcePartitions = new ConcurrentHashMap<>();

  protected final KafkaBasedConnectorTaskMetrics _consumerMetrics;

  private volatile int _pollAttempts;

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
    if (Boolean.TRUE.toString()
        .equals(_datastream.getMetadata().get(KafkaDatastreamMetadataConstants.USE_PASSTHROUGH_COMPRESSION))) {
      _consumerProps.put("enable.shallow.iterator", Boolean.TRUE.toString());
      _logger.info("Enabled passthrough compression for task: {} Datastream: {}", task.getDatastreamTaskName(),
          _datastreamName);
    }

    _processingDelayLogThresholdMillis = config.getProcessingDelayLogThresholdMillis();
    _maxRetryCount = config.getRetryCount();
    _pausePartitionOnError = config.getPausePartitionOnError();
    _pauseErrorPartitionDuration = config.getPauseErrorPartitionDuration();
    _startOffsets = Optional.ofNullable(_datastream.getMetadata().get(DatastreamMetadataConstants.START_POSITION))
        .map(json -> JsonUtils.fromJson(json, new TypeReference<Map<Integer, Long>>() {
        }));
    _offsetCommitInterval = config.getCommitIntervalMillis();
    _pollTimeoutMillis = config.getPollTimeoutMillis();
    _retrySleepDuration = config.getRetrySleepDuration();
    _commitTimeout = config.getCommitTimeout();
    _consumerMetrics = createKafkaBasedConnectorTaskMetrics(metricsPrefix, _datastreamName, _logger);

    _pollAttempts = 0;
    _kafkaPositionTracker = Optional.ofNullable(createKafkaPositionTracker(config));
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
    // iterate through each topic partition one at a time, for better isolation
    for (TopicPartition topicPartition : records.partitions()) {
      for (ConsumerRecord<?, ?> record : records.records(topicPartition)) {
        try {
          if (_autoPausedSourcePartitions.containsKey(topicPartition)) {
            _logger.warn("Abort sending as {} is auto-paused", topicPartition);
          } else {
            DatastreamProducerRecord datastreamProducerRecord = translate(record, readTime);
            int numBytes = record.serializedKeySize() + record.serializedValueSize();
            sendDatastreamProducerRecord(datastreamProducerRecord, topicPartition, numBytes);
          }
        } catch (Exception e) {
          _logger.warn("Got exception while sending record {}", record);
          rewindAndPausePartitionOnException(topicPartition, e);
          // skip other messages for this partition, but can continue processing other partitions
          break;
        }
      }
    }
  }

  protected void rewindAndPausePartitionOnException(TopicPartition srcTopicPartition, Exception ex) {
    _consumerMetrics.updateErrorRate(1);
    Instant start = Instant.now();
    // seek to previous checkpoints for this topic partition
    boolean partitionRewound = false;
    try {
      seekToLastCheckpoint(Collections.singleton(srcTopicPartition));
      partitionRewound = true;
    } catch (Exception e) {
      _logger.error("Partition rewind failed due to ", e);
    }
    if (_pausePartitionOnError && (!partitionRewound || !containsTransientException(ex))) {
      // if doesn't contain DatastreamTransientException and it's configured to pause partition on error conditions,
      // add to auto-paused set
      _logger.warn("Adding source topic partition {} to auto-pause set", srcTopicPartition);
      _autoPausedSourcePartitions.put(srcTopicPartition,
          PausedSourcePartitionMetadata.sendError(start, _pauseErrorPartitionDuration, ex));
      _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
    }
  }

  private boolean containsTransientException(Throwable ex) {
    while (ex != null && ex instanceof DatastreamRuntimeException) {
      if (ex instanceof DatastreamTransientException) {
        return true;
      }
      ex = ex.getCause();
    }
    return false;
  }

  protected void sendDatastreamProducerRecord(DatastreamProducerRecord datastreamProducerRecord,
      TopicPartition srcTopicPartition, int numBytes) {
    _producer.send(datastreamProducerRecord, ((metadata, exception) -> {
      if (exception != null) {
        _logger.warn("Detect exception being throw from callback for src partition: {} while sending producer "
          + "record: {}, exception: ", srcTopicPartition, datastreamProducerRecord, exception);
        rewindAndPausePartitionOnException(srcTopicPartition, exception);
      } else {
        _consumerMetrics.updateBytesProcessedRate(numBytes);
      }
    }));
  }

  @Override
  public void run() {
    _logger.info("Starting the Kafka-based connector task for {}", _datastreamTask);
    _connectorTaskThread = Thread.currentThread();
    boolean startingUp = true;
    long pollInterval = 0; // so 1st call to poll is fast for purposes of startup

    _eventsProcessedCountLoggedTime = Instant.now();

    try {
      _consumer = createKafkaConsumer(_consumerProps);
      consumerSubscribe();

      ConsumerRecords<?, ?> records;
      while (!_shutdown) {
        // perform any pre-computations before poll()
        preConsumerPollHook();

        // read a batch of records
        records = pollRecords(pollInterval);
        // handle startup notification if this is the 1st poll call
        if (startingUp) {
          pollInterval = _pollTimeoutMillis;
          startingUp = false;
          _startedLatch.countDown();
        }

        int recordsPolled = 0;
        if (records != null && !records.isEmpty()) {
          Instant readTime = Instant.now();
          processRecords(records, readTime);
          recordsPolled = records.count();
        }
        maybeCommitOffsets(_consumer, false);
        trackEventsProcessedProgress(recordsPolled);

      } // end while loop

      // shutdown, do a force commit
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
   * Signal task to stop
   */
  public void stop() {
    _logger.info("{} stopping", _taskName);
    _shutdown = true;
    if (_consumer != null) {
      _logger.info("Waking up the consumer for task {}", _taskName);
      _consumer.wakeup();
    }
    _consumerMetrics.deregisterMetrics();
  }

  /**
   * Wait till the task is started or given timeout is reached
   * @param timeout Time to wait
   * @param unit The time unit of given timeout
   * @return true if the task is started in given time
   */
  public boolean awaitStart(long timeout, TimeUnit unit) throws InterruptedException {
    return _startedLatch.await(timeout, unit);
  }

  /**
   * Wait till the task is shut down or given timeout is reached
   * @param timeout Time to wait
   * @param unit The time unit of given timeout
   * @return true if the task stopped within the specified timeout
   */
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
   * @throws Exception if the poll has failed too many times
   */
  protected ConsumerRecords<?, ?> pollRecords(long pollInterval) throws Exception {
    ConsumerRecords<?, ?> records;
    try {
      long curPollTime = System.currentTimeMillis();
      _lastPolledTimeMillis = curPollTime;
      records = _consumer.poll(pollInterval);
      long pollDurationMillis = System.currentTimeMillis() - curPollTime;
      if (pollDurationMillis > pollInterval + POLL_BUFFER_TIME_MILLIS) {
        // record poll time exceeding client poll timeout
        _logger.warn("ConsumerId: {}, Kafka client poll took {} ms (> poll timeout {} + buffer time {} ms)", _taskName,
            pollDurationMillis, pollInterval, POLL_BUFFER_TIME_MILLIS);
        _consumerMetrics.updateClientPollOverTimeout(1);
      }
      _consumerMetrics.updateNumPolls(1);
      _consumerMetrics.updateEventCountsPerPoll(records.count());
      if (!records.isEmpty()) {
        _consumerMetrics.updateEventsProcessedRate(records.count());
        _consumerMetrics.updateLastEventReceivedTime(Instant.now());
      }
      _pollAttempts = 0;

      try {
        _kafkaPositionTracker.ifPresent(tracker -> {
          // The calls to position() (needed for initializing the position data for these positions) are intentionally
          // made after a poll() so that they will return very quickly
          for (final TopicPartition topicPartition : tracker.getPartitionsNeedingInit()) {
            try {
              tracker.initializePartition(topicPartition, _consumer.position(topicPartition));
            } catch (IllegalStateException e) {
              // Would occur if the partition has been unassigned but onPartitionsRevoked() has not yet been called.
              // In this case, there is nothing we can do but wait for onPartitionsRevoked().
              _logger.trace("Got IllegalStateException when processing partition {}", topicPartition, e);
            } catch (InvalidOffsetException e) {
              // Occurs if no offset is defined for the partition and no offset reset policy is defined.
              // This error should have been caught by poll(), but will definitely be caught by poll() in the next run,
              // so it should be safe to ignore this exception and allow records processing to continue.
              _logger.trace("Got InvalidOffsetException when processing partition {}", topicPartition, e);
            }
          }
          tracker.onRecordsReceived(records, _consumer.metrics());
        });
      } catch (WakeupException | InterruptException e) {
        throw e;
      } catch (Exception e) {
        _logger.warn("Got uncaught exception while processing position tracker code (swallowing and continuing)", e);
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

  private void sendPollInfoToPositionTracker() {
    _kafkaPositionTracker.ifPresent(tracker -> {

    });
  }

  protected long getLastPolledTimeMillis() {
    return _lastPolledTimeMillis;
  }

  /**
   * Processes the Kafka consumer records by translating them and sending them to the event producer.
   * @param records the Kafka consumer records
   * @param readTime the time at which the records were successfully polled from Kafka
   */
  protected void processRecords(ConsumerRecords<?, ?> records, Instant readTime) {
    // send the batch out the other end
    translateAndSendBatch(records, readTime);

    if (System.currentTimeMillis() - readTime.toEpochMilli() > _processingDelayLogThresholdMillis) {
      _consumerMetrics.updateProcessingAboveThreshold(1);
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
      // Seek to start position, by default we are starting from latest one as we just start consumption
      seekToStartPosition(_consumer, e.partitions(),
          _consumerProps.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_AUTO_OFFSET_RESET_CONFIG_LATEST));
    }
  }

  /**
   * Handle exception during pollRecords(). The base behavior is to sleep for some time.
   * @param e the Exception
   */
  protected void handlePollRecordsException(Exception e) throws Exception {
    _pollAttempts++;
    if (_maxRetryCount > 0 && _pollAttempts > _maxRetryCount) {
      _logger.warn("Poll fail with exception", e);
      throw e;
    } else {
      _logger.warn("Poll threw an exception. Sleeping for {} seconds and repoll from consumer, poll attempt: {}",
          _retrySleepDuration.getSeconds(), _pollAttempts, e);
    }
    if (!_shutdown) {
      Thread.sleep(_retrySleepDuration.toMillis());
    }
  }

  abstract protected void maybeCommitOffsets(Consumer<?, ?> consumer, boolean force);

  /**
   * Flush the producer and commit the offsets if the configured offset commit interval has been reached, or if
   * force is set to true.
   * @param consumer the Kafka consumer
   * @param force whether flush and commit should happen unconditionally
   */
  protected void maybeCommitOffsetsInternal(Consumer<?, ?> consumer, boolean force) {
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
          consumer.commitSync(offsets.get(), _commitTimeout);
        } else {
          consumer.commitSync(_commitTimeout);
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
    }, COMMIT_RETRY_INTERVAL_MILLIS, COMMIT_RETRY_TIMEOUT_MILLIS);

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
    // reset consumer to last checkpoint, by default we will rewind the checkpoint
    lastCheckpoint.forEach((tp, offsetAndMetadata) -> _consumer.seek(tp, offsetAndMetadata.offset()));
    if (!tpWithNoCommits.isEmpty()) {
      _logger.info("Seeking to start position for partitions: {}", tpWithNoCommits);
      seekToStartPosition(_consumer, tpWithNoCommits,
          _consumerProps.getProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, CONSUMER_AUTO_OFFSET_RESET_CONFIG_EARLIEST));
    }
  }

  private void seekToStartPosition(Consumer<?, ?> consumer, Set<TopicPartition> partitions, String strategy) {
    // We would like to consume from latest when there is no offset. However, if we rewind due to exception, we want
    // to rewind to earliest to make sure the messages which are added after we start consuming don't get skipped
    if (_startOffsets.isPresent()) {
      _logger.info("Datastream is configured with StartPosition. Trying to start from {}", _startOffsets.get());
      seekToOffset(consumer, partitions, _startOffsets.get());
    } else {
      if (CONSUMER_AUTO_OFFSET_RESET_CONFIG_LATEST.equals(strategy)) {
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

  private void updateConsumerAssignment(Collection<TopicPartition> partitions) {
    _consumerAssignment.clear();
    _consumerAssignment.addAll(partitions);
    _consumerMetrics.updateNumPartitions(_consumerAssignment.size());
    _consumerMetrics.updateNumTopics(_consumerAssignment.stream().map(tp -> tp.topic()).distinct().count());
    _logger.info("Current assignment is {}", _consumerAssignment);
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> topicPartitions) {
    _logger.info("Partition ownership revoked for {}, checkpointing.", topicPartitions);
    _kafkaPositionTracker.ifPresent(tracker -> tracker.onPartitionsRevoked(topicPartitions));
    if (!_shutdown && !topicPartitions.isEmpty()) { // there is a commit at the end of the run method, skip extra commit in shouldDie mode.
      try {
        maybeCommitOffsets(_consumer, true); // happens inline as part of poll
      } catch (Exception e) {
        // log the exception and let the new partition owner just read from previous checkpoint
        _logger.warn("Caught exception while trying to commit offsets in onPartitionsRevoked with partitions {}.",
            topicPartitions, e);
      }
    }

    updateConsumerAssignment(_consumer.assignment());

    // update paused partitions
    _taskUpdates.add(DatastreamConstants.UpdateType.PAUSE_RESUME_PARTITIONS);
  }

  @Override
  public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
    _logger.info("Partition ownership assigned for {}.", partitions);
    _kafkaPositionTracker.ifPresent(tracker -> tracker.onPartitionsAssigned(partitions));
    _consumerMetrics.updateRebalanceRate(1);

    updateConsumerAssignment(partitions);

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
    }

    // update paused partition metrics
    long numAutoPausedPartitionsOnError = 0;
    long numAutoPausedPartitionsAwaitingDestTopic = 0;
    long numAutoPausedPartitionsOnInFlightMessages = 0;
    for (PausedSourcePartitionMetadata metadata : _autoPausedSourcePartitions.values()) {
      switch (metadata.getReason()) {
        case SEND_ERROR:
          numAutoPausedPartitionsOnError++;
          break;
        case EXCEEDED_MAX_IN_FLIGHT_MSG_THRESHOLD:
          numAutoPausedPartitionsOnInFlightMessages++;
          break;
        case TOPIC_NOT_CREATED:
          numAutoPausedPartitionsAwaitingDestTopic++;
          break;
        default:
          break;
      }
    }
    _consumerMetrics.updateNumAutoPausedPartitionsOnError(numAutoPausedPartitionsOnError);
    _consumerMetrics.updateNumAutoPausedPartitionsOnInFlightMessages(numAutoPausedPartitionsOnInFlightMessages);
    _consumerMetrics.updateNumAutoPausedPartitionsAwaitingDestTopic(numAutoPausedPartitionsAwaitingDestTopic);
    _consumerMetrics.updateNumConfigPausedPartitions(partitionsToPause.size() - _autoPausedSourcePartitions.size());
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

  /**
   * Get Brooklin metrics info
   * @param prefix Prefix to prepend to metrics
   */
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

  /**
   * Check if this task belongs to given datastream
   * @param datastreamName Name of the datastream that needs to be checked.
   * @return true if task belongs to given datastream
   */
  public boolean hasDatastream(String datastreamName) {
    return _datastreamName.equals(datastreamName);
  }

  /**
   * Get group ID for given task from metadata (if present), null otherwise.
   *
   * @param task Task whose group ID needs to be found out.
   * @param consumerMetrics CommonConnectorMetrics instance for any errors that need to be reported.
   * @param logger Logger instance to log information.
   * @return Group ID if present, null otherwise.
   */
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
      logger.info("Found overridden group ID for Kafka datastream task: {}. Overridden group id: {} Datastreams: {}",
          task.getId(), groupIds.toArray()[0], task.getDatastreams());
      return (String) groupIds.toArray()[0];
    }

    return null;
  }

  /**
   * Creates a KafkaPositionTracker if enabled in the provided config.
   *
   * @param config the provided config
   * @return a KafkaPositionTracker if enabled in config, or null
   */
  private KafkaPositionTracker createKafkaPositionTracker(KafkaBasedConnectorConfig config) {
    if (config.getEnablePositionTracker()) {
      final String brooklinConnectorName = _datastreamTask.getConnectorType();
      final String brooklinTaskPrefix = _datastreamTask.getTaskPrefix();
      final String brooklinTaskId = _datastreamTask.getDatastreamTaskName();
      final Instant taskStartTime = Instant.now();
      final Supplier<Boolean> isConnectorTaskAlive = () -> !_shutdown
          && (_connectorTaskThread == null || _connectorTaskThread.isAlive());
      final Supplier<Consumer<?, ?>> consumerSupplier = () -> createKafkaConsumer(_consumerProps);
      return new KafkaPositionTracker(brooklinConnectorName, brooklinTaskPrefix, brooklinTaskId,
          taskStartTime, config.getEnableBrokerOffsetFetcher(), isConnectorTaskAlive, consumerSupplier);
    }
    return null;
  }
}
