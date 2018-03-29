package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.util.Properties;

import com.google.common.annotations.VisibleForTesting;

import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ReflectionUtils;
import com.linkedin.datastream.common.VerifiableProperties;


/**
 * Configs for Kafka-based connectors.
 */
public class KafkaBasedConnectorConfig {

  public static final String DOMAIN_KAFKA_CONSUMER = "consumer";
  public static final String CONFIG_COMMIT_INTERVAL_MILLIS = "commitIntervalMs";
  public static final String CONFIG_CONSUMER_FACTORY_CLASS = "consumerFactoryClassName";
  public static final String CONFIG_DEFAULT_KEY_SERDE = "defaultKeySerde";
  public static final String CONFIG_DEFAULT_VALUE_SERDE = "defaultValueSerde";
  public static final String CONFIG_RETRY_COUNT = "retryCount";
  public static final String CONFIG_RETRY_SLEEP_DURATION_MS = "retrySleepDurationMs";
  public static final String CONFIG_PAUSE_PARTITION_ON_ERROR = "pausePartitionOnError";
  public static final String CONFIG_PAUSE_ERROR_PARTITION_DURATION_MS = "pauseErrorPartitionDurationMs";
  public static final String DAEMON_THREAD_INTERVAL_SECONDS = "daemonThreadIntervalInSeconds";
  public static final String NON_GOOD_STATE_THRESHOLD_MS = "nonGoodStateThresholdMs";

  private static final long DEFAULT_RETRY_SLEEP_DURATION_MS = Duration.ofSeconds(5).toMillis();
  private static final long DEFAULT_PAUSE_ERROR_PARTITION_DURATION_MS = Duration.ofMinutes(10).toMillis();
  private static final int DEFAULT_RETRY_COUNT = 5;
  private static final int DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS = 300;
  public static final long DEFAULT_NON_GOOD_STATE_THRESHOLD_MS = Duration.ofMinutes(10).toMillis();
  public static final long MIN_NON_GOOD_STATE_THRESHOLD_MS = Duration.ofMinutes(1).toMillis();

  private final Properties _consumerProps;
  private final VerifiableProperties _connectorProps;
  private final KafkaConsumerFactory<?, ?> _consumerFactory;

  private final String _defaultKeySerde;
  private final String _defaultValueSerde;
  private final long _commitIntervalMillis;
  private final int _retryCount;
  private final Duration _retrySleepDuration;
  private final boolean _pausePartitionOnError;
  private final Duration _pauseErrorPartitionDuration;

  private final int _daemonThreadIntervalSeconds;
  private final long _nonGoodStateThresholdMs;

  public KafkaBasedConnectorConfig(Properties properties) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    _defaultKeySerde = verifiableProperties.getString(CONFIG_DEFAULT_KEY_SERDE, "");
    _defaultValueSerde = verifiableProperties.getString(CONFIG_DEFAULT_VALUE_SERDE, "");
    _commitIntervalMillis =
        verifiableProperties.getLongInRange(CONFIG_COMMIT_INTERVAL_MILLIS, Duration.ofMinutes(1).toMillis(), 0,
            Long.MAX_VALUE);
    _retryCount = verifiableProperties.getInt(CONFIG_RETRY_COUNT, DEFAULT_RETRY_COUNT);
    _retrySleepDuration = Duration.ofMillis(
        verifiableProperties.getLong(CONFIG_RETRY_SLEEP_DURATION_MS, DEFAULT_RETRY_SLEEP_DURATION_MS));
    _pausePartitionOnError = verifiableProperties.getBoolean(CONFIG_PAUSE_PARTITION_ON_ERROR, Boolean.FALSE);
    _pauseErrorPartitionDuration = Duration.ofMillis(
        verifiableProperties.getLong(CONFIG_PAUSE_ERROR_PARTITION_DURATION_MS,
            DEFAULT_PAUSE_ERROR_PARTITION_DURATION_MS));
    _daemonThreadIntervalSeconds = verifiableProperties.getInt(DAEMON_THREAD_INTERVAL_SECONDS, DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS);
    _nonGoodStateThresholdMs = verifiableProperties.getLongInRange(NON_GOOD_STATE_THRESHOLD_MS, DEFAULT_NON_GOOD_STATE_THRESHOLD_MS,
        MIN_NON_GOOD_STATE_THRESHOLD_MS, Long.MAX_VALUE);

    String factory =
        verifiableProperties.getString(CONFIG_CONSUMER_FACTORY_CLASS, KafkaConsumerFactoryImpl.class.getName());
    _consumerFactory = ReflectionUtils.createInstance(factory);
    if (_consumerFactory == null) {
      throw new DatastreamRuntimeException("Unable to instantiate factory class: " + factory);
    }

    _consumerProps = verifiableProperties.getDomainProperties(DOMAIN_KAFKA_CONSUMER);
    _connectorProps = verifiableProperties;
  }

  @VisibleForTesting
  public KafkaBasedConnectorConfig(KafkaConsumerFactory<?, ?> consumerFactory, VerifiableProperties connectorProps,
      Properties consumerProps, String defaultKeySerde, String defaultValueSerde, long commitIntervalMillis,
      int retryCount, Duration retrySleepDuration, boolean pausePartitionOnError,
      Duration pauseErrorPartitionDuration) {
    _consumerFactory = consumerFactory;
    _connectorProps = connectorProps;
    _consumerProps = consumerProps;

    _defaultKeySerde = defaultKeySerde;
    _defaultValueSerde = defaultValueSerde;
    _commitIntervalMillis = commitIntervalMillis;
    _retryCount = retryCount;
    _retrySleepDuration = retrySleepDuration;
    _pausePartitionOnError = pausePartitionOnError;
    _pauseErrorPartitionDuration = pauseErrorPartitionDuration;
    _daemonThreadIntervalSeconds = DEFAULT_DAEMON_THREAD_INTERVAL_SECONDS;
    _nonGoodStateThresholdMs = DEFAULT_NON_GOOD_STATE_THRESHOLD_MS;
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

  public long getNonGoodStateThresholdMs() {
    return _nonGoodStateThresholdMs;
  }
}
