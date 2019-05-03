/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;


import java.time.Duration;
import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;

import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_COMMIT_INTERVAL_MILLIS;
import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_COMMIT_TIMEOUT_MILLIS;
import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_ENABLE_KAFKA_POSITION_TRACKER;
import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_PAUSE_ERROR_PARTITION_DURATION_MILLIS;
import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_PAUSE_PARTITION_ON_ERROR;
import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_POLL_TIMEOUT_MILLIS;
import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_RETRY_COUNT;
import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig.CONFIG_RETRY_SLEEP_DURATION_MILLIS;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;


/**
 * A test utility to facilitate building {@link KafkaBasedConnectorConfig} objects.
 */
public class KafkaBasedConnectorConfigBuilder {

  private final Properties _properties = new Properties();

  private KafkaConsumerFactory<?, ?> _consumerFactory;
  private Properties _consumerProps;
  private Properties _connectorProps;

  /**
   * Constructs KafkaBasedConnectorConfigBuilder with defaults that work for most test cases
   */
  public KafkaBasedConnectorConfigBuilder() {
    setCommitIntervalMillis(1000);
    setPollTimeoutMillis(500);
    setRetryCount(5);
    setRetrySleepDuration(Duration.ZERO);
    setPausePartitionOnError(false);
    setPauseErrorPartitionDuration(Duration.ZERO);
    setEnableKafkaPositionTracker(true);
  }

  /**
   * Build KafkaBasedConnectorConfig instance
   */
  public KafkaBasedConnectorConfig build() {
    KafkaBasedConnectorConfig result = spy(new KafkaBasedConnectorConfig(_properties));

    if (_consumerFactory != null) {
      doReturn(_consumerFactory).when(result).getConsumerFactory();
    }

    if (_connectorProps != null) {
      when(result.getConnectorProps()).thenReturn(new VerifiableProperties(_connectorProps));
    }

    if (_consumerProps != null) {
      when(result.getConsumerProps()).thenReturn(_consumerProps);
    }

    return result;
  }

  /**
   * Set Kafka consumer factory
   */
  public KafkaBasedConnectorConfigBuilder setConsumerFactory(KafkaConsumerFactory<?, ?> consumerFactory) {
    _consumerFactory = consumerFactory;
    return this;
  }

  /**
   * Set connector properties
   */
  public KafkaBasedConnectorConfigBuilder setConnectorProps(Properties connectorProps) {
    _connectorProps = connectorProps;
    return this;
  }

  /**
   * Set Kafka consumer properties
   */
  public KafkaBasedConnectorConfigBuilder setConsumerProps(Properties consumerProps) {
    _consumerProps = consumerProps;
    return this;
  }

  /**
   * Set offset commit interval
   */
  public KafkaBasedConnectorConfigBuilder setCommitIntervalMillis(long commitIntervalMillis) {
    _properties.put(CONFIG_COMMIT_INTERVAL_MILLIS, Long.toString(commitIntervalMillis));
    return this;
  }

  /**
   * Set offset commit timeout
   */
  public KafkaBasedConnectorConfigBuilder setCommitTimeout(Duration commitTimeout) {
    _properties.put(CONFIG_COMMIT_TIMEOUT_MILLIS, Long.toString(commitTimeout.toMillis()));
    return this;
  }

  /**
   * Set the retry count
   */
  public KafkaBasedConnectorConfigBuilder setRetryCount(int retryCount) {
    _properties.put(CONFIG_RETRY_COUNT, Integer.toString(retryCount));
    return this;
  }

  /**
   * Set sleep duration between retries
   */
  public KafkaBasedConnectorConfigBuilder setRetrySleepDuration(Duration retrySleepDuration) {
    _properties.put(CONFIG_RETRY_SLEEP_DURATION_MILLIS, Long.toString(retrySleepDuration.toMillis()));
    return this;
  }

  /**
   * Set partition auto-pause on error
   */
  public KafkaBasedConnectorConfigBuilder setPausePartitionOnError(boolean pausePartitionOnError) {
    _properties.put(CONFIG_PAUSE_PARTITION_ON_ERROR, Boolean.toString(pausePartitionOnError));
    return this;
  }

  /**
   * Set partition auto-pause duration
   */
  public KafkaBasedConnectorConfigBuilder setPauseErrorPartitionDuration(Duration pauseErrorPartitionDuration) {
    _properties.put(CONFIG_PAUSE_ERROR_PARTITION_DURATION_MILLIS, Long.toString(pauseErrorPartitionDuration.toMillis()));
    return this;
  }

  /**
   * Set poll timeout
   */
  public KafkaBasedConnectorConfigBuilder setPollTimeoutMillis(long pollTimeoutMillis) {
    _properties.put(CONFIG_POLL_TIMEOUT_MILLIS, Long.toString(pollTimeoutMillis));
    return this;
  }

  /**
   * Set flag indicating whether or not to enable Kafka position tracker
   */
  public KafkaBasedConnectorConfigBuilder setEnableKafkaPositionTracker(boolean enableKafkaPositionTracker) {
    _properties.put(CONFIG_ENABLE_KAFKA_POSITION_TRACKER, Boolean.toString(enableKafkaPositionTracker));
    return this;
  }
}
