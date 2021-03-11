/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.RateLimiter;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.kafka.factory.KafkaProducerFactory;
import com.linkedin.datastream.kafka.factory.SimpleKafkaProducerFactory;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamTask;

import static com.linkedin.datastream.connectors.CommonConnectorMetrics.AGGREGATE;
import static com.linkedin.datastream.kafka.factory.KafkaProducerFactory.DOMAIN_PRODUCER;


class KafkaProducerWrapper<K, V> {
  private static final String CLASS_NAME = KafkaProducerWrapper.class.getSimpleName();
  private static final String PRODUCER_ERROR = "producerError";

  // Default producer configuration for no data loss pipeline.
  private static final String DEFAULT_PRODUCER_ACKS_CONFIG_VALUE = "all";
  private static final String DEFAULT_MAX_BLOCK_MS_CONFIG_VALUE = String.valueOf(Integer.MAX_VALUE);
  private static final String DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_VALUE = "1";

  private static final long DEFAULT_SEND_FAILURE_RETRY_WAIT_MS = Duration.ofSeconds(5).toMillis();
  private static final int DEFAULT_PRODUCER_FLUSH_TIMEOUT_MS = Integer.MAX_VALUE;
  private static final Double DEFAULT_RATE_LIMITER = 0.1;

  private static final String CFG_SEND_FAILURE_RETRY_WAIT_MS = "send.failure.retry.wait.time.ms";
  private static final String CFG_KAFKA_PRODUCER_FACTORY = "kafkaProducerFactory";

  private static final AtomicInteger NUM_PRODUCERS = new AtomicInteger();
  private static final Supplier<Integer> PRODUCER_GAUGE = NUM_PRODUCERS::get;

  private static final int DEFAULT_PRODUCER_CLOSE_TIMEOUT_MS = 10000;
  private static final int FAST_CLOSE_TIMEOUT_MS = 2000;
  private static final int MAX_SEND_ATTEMPTS = 10;

  @VisibleForTesting
  static final String CFG_RATE_LIMITER_CFG = "producerRateLimiter";

  @VisibleForTesting
  static final String PRODUCER_COUNT = "producerCount";

  @VisibleForTesting
  static final String CFG_PRODUCER_FLUSH_TIMEOUT_MS = "producerFlushTimeoutMs";

  @VisibleForTesting
  static final String CFG_PRODUCER_CLOSE_TIMEOUT_MS = "producerCloseTimeoutMs";

  private final Logger _log;
  private final long _sendFailureRetryWaitTimeMs;
  private final int _producerFlushTimeoutMs;
  private final int _producerCloseTimeoutMs;

  private final String _clientId;
  private final Properties _props;

  // Set of datastream tasks assigned to the producer
  private final Set<DatastreamTask> _tasks = ConcurrentHashMap.newKeySet();

  // Producer is lazily initialized during the first send call.
  // Also, can be nullified in case of exceptions, and recreated by subsequent send calls.
  // Mark as volatile as it is mutable and used by different threads
  private volatile Producer<K, V> _kafkaProducer;

  private final KafkaProducerFactory<K, V> _producerFactory;

  // Limiter to control how fast producers are re-created after failures.
  // Note that there is no delay the first time the producer is created,
  // but subsequent calls will be limited to 1 every 10 seconds by default.
  // The reason is to give time to the Kafka producer to release resources and
  // close threads before creating a new one.
  private final RateLimiter _rateLimiter;

  private final DynamicMetricsManager _dynamicMetricsManager;
  private final String _metricsNamesPrefix;

  // A lock used to synchronize access to operations performed on the _kafkaProducer object
  private final Lock _producerLock = new ReentrantLock();
  // A condition to wait on before creating a new producer
  private final Condition _waitOnProducerClose = _producerLock.newCondition();
  // This should be accessed using _producerLock.
  private boolean _closeInProgress = false;

  // An executor to spawn threads to close the producer.
  private final ExecutorService _producerCloseExecutorService = Executors.newSingleThreadExecutor(
      new ThreadFactoryBuilder().setNameFormat("KafkaProducerWrapperClose-%d").build());

  KafkaProducerWrapper(String logSuffix, Properties props) {
    this(logSuffix, props, null);
  }

  KafkaProducerWrapper(String logSuffix, Properties props, String metricsNamesPrefix) {
    _log = LoggerFactory.getLogger(String.format("%s:%s", KafkaTransportProvider.class, logSuffix));

    if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      throw new RuntimeException("Bootstrap servers are not set");
    }

    VerifiableProperties transportProviderProperties = new VerifiableProperties(props);

    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _metricsNamesPrefix = metricsNamesPrefix == null ? CLASS_NAME : metricsNamesPrefix + CLASS_NAME;
    _dynamicMetricsManager.registerGauge(_metricsNamesPrefix, AGGREGATE, PRODUCER_COUNT, PRODUCER_GAUGE);

    _clientId = transportProviderProperties.getProperty(ProducerConfig.CLIENT_ID_CONFIG);
    if (StringUtils.isEmpty(_clientId)) {
      _log.warn("Client ID is either null or empty");
    }

    _sendFailureRetryWaitTimeMs =
        transportProviderProperties.getLong(CFG_SEND_FAILURE_RETRY_WAIT_MS, DEFAULT_SEND_FAILURE_RETRY_WAIT_MS);

    _producerFlushTimeoutMs =
        transportProviderProperties.getInt(CFG_PRODUCER_FLUSH_TIMEOUT_MS, DEFAULT_PRODUCER_FLUSH_TIMEOUT_MS);

    _producerCloseTimeoutMs =
        transportProviderProperties.getInt(CFG_PRODUCER_CLOSE_TIMEOUT_MS, DEFAULT_PRODUCER_CLOSE_TIMEOUT_MS);

    _rateLimiter =
        RateLimiter.create(transportProviderProperties.getDouble(CFG_RATE_LIMITER_CFG, DEFAULT_RATE_LIMITER));

    _props = props;

    String kafkaProducerFactoryName = transportProviderProperties.getString(CFG_KAFKA_PRODUCER_FACTORY,
        SimpleKafkaProducerFactory.class.getCanonicalName());
    _producerFactory = ReflectionUtils.createInstance(kafkaProducerFactoryName);

    populateDefaultProducerConfigs();
  }

  private void populateDefaultProducerConfigs() {
    _props.putIfAbsent(DOMAIN_PRODUCER + "." + ProducerConfig.ACKS_CONFIG, DEFAULT_PRODUCER_ACKS_CONFIG_VALUE);
    _props.putIfAbsent(DOMAIN_PRODUCER + "." + ProducerConfig.MAX_BLOCK_MS_CONFIG, DEFAULT_MAX_BLOCK_MS_CONFIG_VALUE);
    _props.putIfAbsent(DOMAIN_PRODUCER + "." + ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
        DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION_VALUE);
  }

  private Optional<Producer<K, V>> maybeGetKafkaProducer(DatastreamTask task) {
    if (!_tasks.contains(task)) {
      _log.warn("Task {} has been unassigned for producer, abort the send", task);
      return Optional.empty();
    }

    Producer<K, V> producer = _kafkaProducer;
    if (producer == null || _closeInProgress) {
      try {
        producer = initializeProducer();
      } catch (InterruptedException e) {
        _log.warn("Got interrupted while trying to initialize the producer for task {}", task);
      }
    }
    return Optional.ofNullable(producer);
  }

  void assignTask(DatastreamTask task) {
    _tasks.add(task);
  }

  void unassignTask(DatastreamTask task) {
    unassignTasks(Collections.singletonList(task));
  }

  void unassignTasks(List<DatastreamTask> taskList) {
    boolean taskPresent = _tasks.removeAll(taskList);
    try {
      _producerLock.lock();

      // whenever a task is unassigned the kafka producer should be shutdown to ensure that there are no
      // pending sends. Further sends will fail until the producer is re-initialized by a valid task.
      if (taskPresent && _kafkaProducer != null && !_closeInProgress) {
        shutdownProducer();
      }
    } finally {
      _producerLock.unlock();
    }
  }

  int getTasksSize() {
    return _tasks.size();
  }

  private Producer<K, V> initializeProducer() throws InterruptedException {
    // Must be protected by a lock to avoid creating duplicate producers when multiple concurrent
    // sends are in-flight and _kafkaProducer has been set to null as a result of previous
    // producer exception.
    try {
      _producerLock.lock();
      // make sure there is no close in progress.
      int attemptCount = 1;
      while (_closeInProgress) {
        boolean closeCompleted = _waitOnProducerClose.await(_producerCloseTimeoutMs, TimeUnit.MILLISECONDS);
        if (!closeCompleted) {
          _log.warn("Cannot initialize new producer because close is in progress. Retry again. Attempt: {}", attemptCount++);
        }
      }

      if (_kafkaProducer == null) {
        _rateLimiter.acquire();
        _kafkaProducer = createKafkaProducer();
        NUM_PRODUCERS.incrementAndGet();
      }
      return _kafkaProducer;
    } finally {
      _producerLock.unlock();
    }
  }

  @VisibleForTesting
  Producer<K, V> createKafkaProducer() {
    return _producerFactory.createProducer(_props);
  }

  void send(DatastreamTask task, ProducerRecord<K, V> producerRecord, Callback onComplete)
      throws InterruptedException {
    // There are two known cases that lead to IllegalStateException and we should retry:
    //  1) number of brokers is less than minISR
    //  2) producer is closed in generateSendFailure by another thread
    // For either condition, we should retry as broker comes back healthy or producer is recreated
    boolean retry = true;
    int numberOfAttempt = 0;
    while (retry) {
      try {
        ++numberOfAttempt;
        Optional<Producer<K, V>> producer = maybeGetKafkaProducer(task);
        if (producer.isPresent()) {
          producer.get().send(producerRecord, (metadata, exception) -> {
            if (exception == null) {
              onComplete.onCompletion(metadata, null);
            } else {
              onComplete.onCompletion(metadata, generateSendFailure(exception, task));
            }
          });
        } else {
          throw new DatastreamRuntimeException(String.format("kafka producer not available for the task: %s", task.getDatastreamTaskName()));
        }

        retry = false;
      } catch (IllegalStateException e) {
        //The following exception should be quite rare as most exceptions will be throw async callback
        _log.warn(String.format("Either send is called on a closed producer or broker count is less than minISR, "
                + "retry in %d ms.", _sendFailureRetryWaitTimeMs), e);
        Thread.sleep(_sendFailureRetryWaitTimeMs);
      } catch (TimeoutException e) {
        _log.warn(String.format("Kafka producer buffer is full, retry in %d ms.", _sendFailureRetryWaitTimeMs), e);
        Thread.sleep(_sendFailureRetryWaitTimeMs);
      } catch (KafkaException e) {
        Throwable cause = e.getCause();
        while (cause instanceof KafkaException) {
          cause = cause.getCause();
        }
        // Set a max_send_attempts for KafkaException as it may be non-recoverable
        if (numberOfAttempt > MAX_SEND_ATTEMPTS || ((cause instanceof Error || cause instanceof RuntimeException))) {
          _log.error(String.format("Send failed for partition %d with a non-retriable exception",
              producerRecord.partition()), e);
          throw generateSendFailure(e, task);
        } else {
          _log.warn(String.format(
              "Send failed for partition %d with a retriable exception, retry %d out of %d in %d ms.",
              producerRecord.partition(), numberOfAttempt, MAX_SEND_ATTEMPTS, _sendFailureRetryWaitTimeMs), e);
          Thread.sleep(_sendFailureRetryWaitTimeMs);
        }
      } catch (DatastreamRuntimeException e) {
        throw generateSendFailure(e, task);
      } catch (Exception e) {
        _log.error(String.format("Send failed for partition %d with an exception: ", producerRecord.partition()), e);
        throw generateSendFailure(e, task);
      }
    }
  }

  private void shutdownProducer() {
    shutdownProducer(false);
  }


  // fastClose should be set to true in the case, where the producer is already in a bad state or has returned error
  // on send callback (to ensure that the records are produce in order). Closing the producer with a shorter timeout
  // can result in records produced, but no delivery of acks from Kafka. This can result in overcounting and should be
  // done only in critical cases.
  @VisibleForTesting
  void shutdownProducer(boolean fastClose) {
    Producer<K, V> producer;
    try {
      _producerLock.lock();
      // if there is no producer or the producer close is in progress, return.
      if (_kafkaProducer == null || _closeInProgress) {
        return;
      }
      producer = _kafkaProducer;
      _closeInProgress = true;

      // This may be called from the send callback. The callbacks are called from the sender thread, and must complete
      // quickly to avoid delaying/blocking the sender thread. Thus schedule the actual producer.close() on a separate
      // thread
      _producerCloseExecutorService.submit(() -> {
        int timeout = fastClose ? FAST_CLOSE_TIMEOUT_MS : _producerCloseTimeoutMs;
        _log.info("KafkaProducerWrapper: Closing the Kafka Producer with timeout: {}", timeout);
        try {
          producer.close(timeout, TimeUnit.MILLISECONDS);
          NUM_PRODUCERS.decrementAndGet();
          _log.info("KafkaProducerWrapper: Kafka Producer is closed");
        } finally {
          markProducerCloseComplete();
        }
      });
    } finally {
      _producerLock.unlock();
    }
  }

  private void markProducerCloseComplete() {
    try {
      _producerLock.lock();
      _closeInProgress = false;
      _kafkaProducer = null;
      _waitOnProducerClose.signalAll();
    } finally {
      _producerLock.unlock();
    }
  }

  private DatastreamRuntimeException generateSendFailure(Exception exception, DatastreamTask task) {
    _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, AGGREGATE, PRODUCER_ERROR, 1);
    if (exception instanceof IllegalStateException) {
      _log.debug("Send failed transiently with exception: ", exception);
      return new DatastreamTransientException(exception);
    } else {
      _log.debug("Send failed with a non-transient exception. Shutting down producer, exception: ", exception);
      if (_tasks.contains(task)) {
        shutdownProducer(true);
      }
      return new DatastreamRuntimeException(exception);
    }
  }

  /*
   * Kafka producer wraps all the exceptions and throws IllegalStateException. So, if the close is
   * in progress, instead of attempting to close the producer, wait for the close to finish.
   * If the close does not finish within the timeout or the thread gets interrupted, then throw the exception.
   * Otherwise, consider that the flush has completed successfully.
   *
   * For any other exception thrown by kafka producer, shutdown the producer to avoid reusing the same producer.
   */
  void flush() {
    Producer<K, V> producer;
    try {
      _producerLock.lock();
      producer = _kafkaProducer;
    } finally {
      _producerLock.unlock();
    }

    if (producer != null) {
      try {
        producer.flush(_producerFlushTimeoutMs, TimeUnit.MILLISECONDS);
        _log.info("Flush completed for the producer, closeInProgress: {}", _closeInProgress);
      } catch (Exception e) {
        _log.warn("Hitting Exception during kafka producer flush.", e);
        // The KafkaProducer object should not be reused on an interrupted/timed out flush. To be safe, we try to
        // close the producer on any exception.
        try {
          _producerLock.lock();
          if (producer == _kafkaProducer) {
            _log.warn("Kafka producer flush may be interrupted/timed out, closing producer {}.", producer);
            shutdownProducer(true);
          } else {
            _log.warn("Kafka producer flush may be interrupted/timed out, producer {} already closed.", producer);
          }
          throw e;
        } finally {
          _producerLock.unlock();
        }
      }
    }
  }

  // This function is called during shutdown or on session expiry (which calls unassignTasks as well).
  // So, it is okay to shutdown the producer once when all the tasks are closed.
  void close(DatastreamTask task) {
    if (_tasks.remove(task) && _tasks.isEmpty()) {
      shutdownProducer();
    }
  }

  static List<BrooklinMetricInfo> getMetricDetails(String metricsNamesPrefix) {
    String prefix = metricsNamesPrefix == null ? CLASS_NAME + MetricsAware.KEY_REGEX
        : metricsNamesPrefix + CLASS_NAME + MetricsAware.KEY_REGEX;

    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.add(new BrooklinMeterInfo(prefix + PRODUCER_ERROR));
    metrics.add(new BrooklinGaugeInfo(prefix + PRODUCER_COUNT));
    return Collections.unmodifiableList(metrics);
  }

  @VisibleForTesting
  Properties getProperties() {
    Properties props = new Properties();
    props.putAll(_props);
    return props;
  }

  @VisibleForTesting
  void setCloseInProgress(boolean closeInProgress) {
    _closeInProgress = closeInProgress;
  }

  public String getClientId() {
    return _clientId;
  }

  /**
   * Get the metrics value from producer for monitoring
   */
  public Optional<Double> getProducerMetricValue(MetricName metricName) {
    return Optional.ofNullable(_kafkaProducer).map(p -> p.metrics().get(metricName)).map(Metric::value);
  }
}
