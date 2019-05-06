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
  public static final String CONFIG_ENABLE_POSITION_TRACKER = "enablePositionTracker";
  public static final String DAEMON_THREAD_INTERVAL_SECONDS = "daemonThreadIntervalInSeconds";
  public static final String NON_GOOD_STATE_THRESHOLD_MILLIS = "nonGoodStateThresholdMs";
  public static final String PROCESSING_DELAY_LOG_THRESHOLD_MILLIS = "processingDelayLogThreshold";
  public static final long DEFAULT_NON_GOOD_STATE_THRESHOLD_MILLIS = Duration.ofMinutes(10).toMillis();
  public static final long MIN_NON_GOOD_STATE_THRESHOLD_MILLIS = Duration.ofMinutes(1).toMillis();

  private static final long DEFAULT_RETRY_SLEEP_DURATION_MILLIS = Duration.ofSeconds(5).toMillis();
  private static final long DEFAULT_PAUSE_ERROR_PARTITION_DURATION_MILLIS = Duration.ofMinutes(10).toMillis();
  private static final long DEFAULT_POLL_TIMEOUT_MILLIS = Duration.ofSeconds(30).toMillis();
  private static final int DEFAULT_RETRY_COUNT = 5;
  private static final int DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS = 300;
  private static final long DEFAULT_PROCESSING_DELAY_LOG_THRESHOLD_MILLIS = Duration.ofMinutes(1).toMillis();
  private static final long DEFAULT_COMMIT_TIMEOUT_MILLIS = Duration.ofSeconds(30).toMillis();

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
  private final boolean _enablePositionTracker;

  private final int _daemonThreadIntervalSeconds;
  private final long _nonGoodStateThresholdMillis;

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
    _enablePositionTracker = verifiableProperties.getBoolean(CONFIG_ENABLE_POSITION_TRACKER, true);

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

  public boolean getEnablePositionTracker() {
    return _enablePositionTracker;
  }
}
