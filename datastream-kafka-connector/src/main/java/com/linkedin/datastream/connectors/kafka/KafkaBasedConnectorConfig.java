/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.util.Properties;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.datastream.kafka.factory.KafkaConsumerFactory;
import com.linkedin.datastream.kafka.factory.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.server.callbackstatus.CallbackStatusFactory;
import com.linkedin.datastream.server.callbackstatus.CallbackStatusWithComparableOffsetsFactory;

/**
 * Configs for Kafka-based connectors.
 */
public class KafkaBasedConnectorConfig {
  public static final String DOMAIN_KAFKA_CONSUMER = "consumer";
  public static final String CONFIG_COMMIT_INTERVAL_MILLIS = "commitIntervalMs";
  public static final String CONFIG_COMMIT_TIMEOUT_MILLIS = "commitTimeoutMs";
  public static final String CONFIG_POLL_TIMEOUT_MILLIS = "pollTimeoutMs";
  public static final String CONFIG_CONSUMER_FACTORY_CLASS = "consumerFactoryClassName";
  public static final String CONFIG_DEFAULT_KEY_SERDE = "defaultKeySerde";
  public static final String CONFIG_DEFAULT_VALUE_SERDE = "defaultValueSerde";
  public static final String CONFIG_RETRY_COUNT = "retryCount";
  public static final String CONFIG_RETRY_SLEEP_DURATION_MILLIS = "retrySleepDurationMs";
  public static final String CONFIG_PAUSE_PARTITION_ON_ERROR = "pausePartitionOnError";
  public static final String CONFIG_PAUSE_ERROR_PARTITION_DURATION_MILLIS = "pauseErrorPartitionDurationMs";
  public static final String ENABLE_ADDITIONAL_METRICS = "enableAdditionalMetrics";
  public static final String INCLUDE_DATASTREAM_NAME_IN_CONSUMER_CLIENT_ID = "includeDatastreamNameInConsumerClientId";
  public static final String DAEMON_THREAD_INTERVAL_SECONDS = "daemonThreadIntervalInSeconds";
  public static final String NON_GOOD_STATE_THRESHOLD_MILLIS = "nonGoodStateThresholdMs";
  public static final String PROCESSING_DELAY_LOG_THRESHOLD_MILLIS = "processingDelayLogThreshold";
  private static final String CONFIG_CALLBACK_STATUS_STRATEGY_FACTORY_CLASS = "callbackStatusStrategyFactoryClass";

  // how long will the connector wait for a task to shut down before interrupting the task thread
  private static final String CONFIG_TASK_INTERRUPT_TIMEOUT_MS = "taskKillTimeoutMs";

  // how long will the connector task wait for producer flush before committing safe offsets during hard commit
  private static final String CONFIG_HARD_COMMIT_FLUSH_TIMEOUT = "hardCommitFlushTimeout";

  // config value to enable Kafka partition management for KafkaMirrorConnector
  public static final String ENABLE_PARTITION_ASSIGNMENT = "enablePartitionAssignment";
  public static final long DEFAULT_NON_GOOD_STATE_THRESHOLD_MILLIS = Duration.ofMinutes(10).toMillis();
  public static final long MIN_NON_GOOD_STATE_THRESHOLD_MILLIS = Duration.ofMinutes(1).toMillis();

  private static final long DEFAULT_RETRY_SLEEP_DURATION_MILLIS = Duration.ofSeconds(5).toMillis();
  private static final long DEFAULT_PAUSE_ERROR_PARTITION_DURATION_MILLIS = Duration.ofMinutes(10).toMillis();
  private static final long DEFAULT_POLL_TIMEOUT_MILLIS = Duration.ofSeconds(30).toMillis();
  private static final int DEFAULT_RETRY_COUNT = 5;
  private static final int DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS = 300;
  private static final long DEFAULT_PROCESSING_DELAY_LOG_THRESHOLD_MILLIS = Duration.ofMinutes(1).toMillis();
  private static final long DEFAULT_COMMIT_TIMEOUT_MILLIS = Duration.ofSeconds(30).toMillis();
  private static final boolean DEFAULT_ENABLE_ADDITIONAL_METRICS = Boolean.TRUE;
  private static final boolean DEFAULT_INCLUDE_DATASTREAM_NAME_IN_CONSUMER_CLIENT_ID = Boolean.FALSE;
  private static final long DEFAULT_TASK_INTERRUPT_TIMEOUT_MS = Duration.ofSeconds(75).toMillis();
  private static final long DEFAULT_HARD_COMMIT_FLUSH_TIMEOUT_MS = Duration.ofSeconds(10).toMillis();
  private static final long POST_TASK_INTERRUPT_TIMEOUT_MS = Duration.ofSeconds(15).toMillis();

  private final Properties _consumerProps;
  private final VerifiableProperties _connectorProps;
  private final KafkaConsumerFactory<?, ?> _consumerFactory;

  private final String _defaultKeySerde;
  private final String _defaultValueSerde;
  private final long _commitIntervalMillis;
  private final Duration _commitTimeout;
  private final long _pollTimeoutMillis;
  private final int _retryCount;
  private final Duration _retrySleepDuration;
  private final boolean _pausePartitionOnError;
  private final Duration _pauseErrorPartitionDuration;
  private final long _processingDelayLogThresholdMillis;
  private final boolean _enableAdditionalMetrics;
  private final boolean _includeDatastreamNameInConsumerClientId;

  private final int _daemonThreadIntervalSeconds;
  private final long _nonGoodStateThresholdMillis;
  private final boolean _enablePartitionAssignment;
  private final long _taskInterruptTimeoutMs;
  private final long _hardCommitFlushTimeoutMs;

  // Kafka based pub sub framework uses Long as their offset type, hence instantiating a Long parameterized factory
  private final CallbackStatusFactory<Long> _callbackStatusStrategyFactory;

  /**
   * Constructor for KafkaBasedConnectorConfig.
   * @param properties Properties to use for creating config.
   */
  public KafkaBasedConnectorConfig(Properties properties) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    _defaultKeySerde = verifiableProperties.getString(CONFIG_DEFAULT_KEY_SERDE, "");
    _defaultValueSerde = verifiableProperties.getString(CONFIG_DEFAULT_VALUE_SERDE, "");
    _commitIntervalMillis =
        verifiableProperties.getLongInRange(CONFIG_COMMIT_INTERVAL_MILLIS, Duration.ofMinutes(1).toMillis(), 0,
            Long.MAX_VALUE);
    _commitTimeout = Duration.ofMillis(
        verifiableProperties.getLongInRange(CONFIG_COMMIT_TIMEOUT_MILLIS, DEFAULT_COMMIT_TIMEOUT_MILLIS, 0,
            Long.MAX_VALUE));
    _pollTimeoutMillis =
        verifiableProperties.getLongInRange(CONFIG_POLL_TIMEOUT_MILLIS, DEFAULT_POLL_TIMEOUT_MILLIS, 0,
            Long.MAX_VALUE);
    _retryCount = verifiableProperties.getInt(CONFIG_RETRY_COUNT, DEFAULT_RETRY_COUNT);
    _retrySleepDuration = Duration.ofMillis(
        verifiableProperties.getLong(CONFIG_RETRY_SLEEP_DURATION_MILLIS, DEFAULT_RETRY_SLEEP_DURATION_MILLIS));
    _pausePartitionOnError = verifiableProperties.getBoolean(CONFIG_PAUSE_PARTITION_ON_ERROR, Boolean.FALSE);
    _pauseErrorPartitionDuration = Duration.ofMillis(
        verifiableProperties.getLong(CONFIG_PAUSE_ERROR_PARTITION_DURATION_MILLIS,
            DEFAULT_PAUSE_ERROR_PARTITION_DURATION_MILLIS));
    _daemonThreadIntervalSeconds =
        verifiableProperties.getInt(DAEMON_THREAD_INTERVAL_SECONDS, DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS);
    _nonGoodStateThresholdMillis =
        verifiableProperties.getLongInRange(NON_GOOD_STATE_THRESHOLD_MILLIS, DEFAULT_NON_GOOD_STATE_THRESHOLD_MILLIS,
            MIN_NON_GOOD_STATE_THRESHOLD_MILLIS, Long.MAX_VALUE);
    _processingDelayLogThresholdMillis =
        verifiableProperties.getLong(PROCESSING_DELAY_LOG_THRESHOLD_MILLIS,
            DEFAULT_PROCESSING_DELAY_LOG_THRESHOLD_MILLIS);
    _enableAdditionalMetrics = verifiableProperties.getBoolean(ENABLE_ADDITIONAL_METRICS,
        DEFAULT_ENABLE_ADDITIONAL_METRICS);
    _includeDatastreamNameInConsumerClientId = verifiableProperties.getBoolean(
        INCLUDE_DATASTREAM_NAME_IN_CONSUMER_CLIENT_ID, DEFAULT_INCLUDE_DATASTREAM_NAME_IN_CONSUMER_CLIENT_ID);
    _enablePartitionAssignment = verifiableProperties.getBoolean(ENABLE_PARTITION_ASSIGNMENT, Boolean.FALSE);
    _taskInterruptTimeoutMs = verifiableProperties.getLong(CONFIG_TASK_INTERRUPT_TIMEOUT_MS, DEFAULT_TASK_INTERRUPT_TIMEOUT_MS);
    _hardCommitFlushTimeoutMs = verifiableProperties.getLong(CONFIG_HARD_COMMIT_FLUSH_TIMEOUT, DEFAULT_HARD_COMMIT_FLUSH_TIMEOUT_MS);

    String callbackStatusStrategyFactoryClass = verifiableProperties.getString(CONFIG_CALLBACK_STATUS_STRATEGY_FACTORY_CLASS,
        CallbackStatusWithComparableOffsetsFactory.class.getName());
    _callbackStatusStrategyFactory = ReflectionUtils.createInstance(callbackStatusStrategyFactoryClass);
    if (_callbackStatusStrategyFactory == null) {
      throw new DatastreamRuntimeException("Unable to instantiate factory class: " + callbackStatusStrategyFactoryClass);
    }

    String factory =
        verifiableProperties.getString(CONFIG_CONSUMER_FACTORY_CLASS, KafkaConsumerFactoryImpl.class.getName());
    _consumerFactory = ReflectionUtils.createInstance(factory);
    if (_consumerFactory == null) {
      throw new DatastreamRuntimeException("Unable to instantiate factory class: " + factory);
    }

    _consumerProps = verifiableProperties.getDomainProperties(DOMAIN_KAFKA_CONSUMER);
    _connectorProps = verifiableProperties;
  }

  public String getDefaultKeySerde() {
    return _defaultKeySerde;
  }

  public String getDefaultValueSerde() {
    return _defaultValueSerde;
  }

  public long getCommitIntervalMillis() {
    return _commitIntervalMillis;
  }

  public Duration getCommitTimeout() {
    return _commitTimeout;
  }

  public long getPollTimeoutMillis() {
    return _pollTimeoutMillis;
  }

  public int getRetryCount() {
    return _retryCount;
  }

  public Duration getRetrySleepDuration() {
    return _retrySleepDuration;
  }

  public boolean getPausePartitionOnError() {
    return _pausePartitionOnError;
  }

  public Duration getPauseErrorPartitionDuration() {
    return _pauseErrorPartitionDuration;
  }

  public boolean getEnableAdditionalMetrics() {
    return _enableAdditionalMetrics;
  }

  public boolean getIncludeDatastreamNameInConsumerClientId() {
    return _includeDatastreamNameInConsumerClientId;
  }

  /**
   * Get all Kafka consumer config properties
   */
  public Properties getConsumerProps() {
    Properties consumerProps = new Properties();
    consumerProps.putAll(_consumerProps);
    return consumerProps;
  }

  public KafkaConsumerFactory<?, ?> getConsumerFactory() {
    return _consumerFactory;
  }

  public VerifiableProperties getConnectorProps() {
    return _connectorProps;
  }

  public int getDaemonThreadIntervalSeconds() {
    return _daemonThreadIntervalSeconds;
  }

  public long getNonGoodStateThresholdMillis() {
    return _nonGoodStateThresholdMillis;
  }

  public long getProcessingDelayLogThresholdMillis() {
    return _processingDelayLogThresholdMillis;
  }

  public boolean getEnablePartitionAssignment() {
    return _enablePartitionAssignment;
  }

  public long getTaskInterruptTimeoutMs() {
    return _taskInterruptTimeoutMs;
  }

  public long getPostTaskInterruptTimeoutMs() {
    return POST_TASK_INTERRUPT_TIMEOUT_MS;
  }

  public long getShutdownExecutorShutdownTimeoutMs() {
    return _taskInterruptTimeoutMs + POST_TASK_INTERRUPT_TIMEOUT_MS;
  }
  public long getHardCommitFlushTimeoutMs() {
    return _hardCommitFlushTimeoutMs;
  }

  public CallbackStatusFactory<Long> getCallbackStatusStrategyFactory() {
    return _callbackStatusStrategyFactory;
  }
}
