/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.util.Properties;

import org.jetbrains.annotations.NotNull;

import com.linkedin.datastream.common.VerifiableProperties;

/**
 * User configurable settings for an instance of KafkaPositionTracker.
 */
public class KafkaPositionTrackerConfig {

  public static final String CONFIG_ENABLE_POSITION_TRACKER = "enablePositionTracker";
  public static final String CONFIG_ENABLE_BROKER_OFFSET_FETCHER = "enableBrokerOffsetFetcher";
  public static final String CONFIG_ENABLE_PARTITION_LEADERSHIP_CALCULATION = "enablePartitionLeadershipCalculation";
  public static final String CONFIG_BROKER_REQUEST_TIMEOUT_MS = "brokerRequestTimeoutMs";
  public static final String CONFIG_BROKER_OFFSETS_FETCH_INTERVAL_MS = "brokerOffsetsFetchIntervalMs";
  public static final String CONFIG_CONSUMER_FAILED_DETECTION_THRESHOLD_MS = "consumerFailedDetectionThresholdMs";
  public static final String CONFIG_BROKER_OFFSETS_FETCH_SIZE = "brokerOffsetsFetchSize";
  public static final String CONFIG_PARTITION_LEADERSHIP_CALCULATION_FREQUENCY = "partitionLeadershipCalcuationFrequencyMs";

  public static final Duration DEFAULT_BROKER_REQUEST_TIMEOUT = Duration.ofMinutes(5);
  public static final Duration DEFAULT_BROKER_OFFSETS_FETCH_INTERVAL = Duration.ofSeconds(30);
  public static final Duration DEFAULT_CONSUMER_FAILED_DETECTION_THRESHOLD = Duration.ofMinutes(30);
  public static final int DEFAULT_BROKER_OFFSETS_FETCH_SIZE = 250;
  public static final Duration DEFAULT_PARTITION_LEADERSHIP_CALCULATION_FREQUENCY = Duration.ofMinutes(5);

  private final boolean _enablePositionTracker;
  private final boolean _enableBrokerOffsetFetcher;
  private final boolean _enablePartitionLeadershipCalculation;
  private final Duration _brokerRequestTimeout;
  private final Duration _brokerOffsetFetcherInterval;
  private final Duration _consumerFailedDetectionThreshold;
  private final int _brokerOffsetsFetchSize;
  private final Duration _partitionLeadershipCalculationFrequency;

  /**
   * Constructor for KafkaPositionTrackerConfig.
   * @param properties The configuration properties to use
   */
  public KafkaPositionTrackerConfig(@NotNull Properties properties) {
    VerifiableProperties verifiableProperties = new VerifiableProperties(properties);
    _enablePositionTracker = verifiableProperties.getBoolean(CONFIG_ENABLE_POSITION_TRACKER, true);
    _enableBrokerOffsetFetcher = verifiableProperties.getBoolean(CONFIG_ENABLE_BROKER_OFFSET_FETCHER, true);
    _enablePartitionLeadershipCalculation =
        verifiableProperties.getBoolean(CONFIG_ENABLE_PARTITION_LEADERSHIP_CALCULATION, true);
    long brokerRequestTimeoutMs = verifiableProperties.getLongInRange(CONFIG_BROKER_REQUEST_TIMEOUT_MS,
        DEFAULT_BROKER_REQUEST_TIMEOUT.toMillis(), 0L, Long.MAX_VALUE);
    _brokerRequestTimeout = Duration.ofMillis(brokerRequestTimeoutMs);
    long brokerOffsetFetcherIntervalMs = verifiableProperties.getLongInRange(CONFIG_BROKER_OFFSETS_FETCH_INTERVAL_MS,
        DEFAULT_BROKER_OFFSETS_FETCH_INTERVAL.toMillis(), 0L, Long.MAX_VALUE);
    _brokerOffsetFetcherInterval = Duration.ofMillis(brokerOffsetFetcherIntervalMs);
    long consumerFailedDetectionThresholdMs =
        verifiableProperties.getLongInRange(CONFIG_CONSUMER_FAILED_DETECTION_THRESHOLD_MS,
            DEFAULT_CONSUMER_FAILED_DETECTION_THRESHOLD.toMillis(), 0L, Long.MAX_VALUE);
    _consumerFailedDetectionThreshold = Duration.ofMillis(consumerFailedDetectionThresholdMs);
    _brokerOffsetsFetchSize = verifiableProperties.getIntInRange(CONFIG_BROKER_OFFSETS_FETCH_SIZE,
        DEFAULT_BROKER_OFFSETS_FETCH_SIZE, 1, Integer.MAX_VALUE);
    long partitionLeadershipCalculationFrequencyMs =
        verifiableProperties.getLongInRange(CONFIG_PARTITION_LEADERSHIP_CALCULATION_FREQUENCY,
            DEFAULT_PARTITION_LEADERSHIP_CALCULATION_FREQUENCY.toMillis(), 0L, Long.MAX_VALUE);
    _partitionLeadershipCalculationFrequency = Duration.ofMillis(partitionLeadershipCalculationFrequencyMs);
  }

  /**
   * True if we should be tracking the consumer and broker's offsets, false otherwise.
   */
  public boolean isEnablePositionTracker() {
    return _enablePositionTracker;
  }

  /**
   * True if we should spin up a separate Kafka consumer and thread to periodically query the broker for its latest
   * offsets. Used to ensure that we are caught up broker consumption even if we haven't explicitly seen data for that
   * partition. False otherwise.
   */
  public boolean isEnableBrokerOffsetFetcher() {
    return _enableBrokerOffsetFetcher;
  }

  /**
   * True if the broker offset fetcher should attempt to calculate partition leadership to improve broker query
   * performance, false otherwise.
   */
  public boolean isEnablePartitionLeadershipCalculation() {
    return _enablePartitionLeadershipCalculation;
  }

  /**
   * The maximum duration that a consumer request is allowed to take before it times out.
   */
  @NotNull
  public Duration getBrokerRequestTimeout() {
    return _brokerRequestTimeout;
  }

  /**
   * The frequency at which to fetch offsets from the broker.
   */
  @NotNull
  public Duration getBrokerOffsetFetcherInterval() {
    return _brokerOffsetFetcherInterval;
  }

  /**
   * The maximum duration that the BrokerOffsetFetcher can go without any successes (indicating that the underlying
   * consumer is faulty and should be fixed).
   */
  @NotNull
  public Duration getConsumerFailedDetectionThreshold() {
    return _consumerFailedDetectionThreshold;
  }

  /**
   * The number of offsets to fetch from the broker per endOffsets() RPC call. This should be chosen to avoid timeouts
   * (larger requests are more likely to cause timeouts).
   */
  public int getBrokerOffsetsFetchSize() {
    return _brokerOffsetsFetchSize;
  }

  /**
   * The frequency at which partition leadership is fetched from the Kafka cluster.
   */
  @NotNull
  public Duration getPartitionLeadershipCalculationFrequency() {
    return _partitionLeadershipCalculationFrequency;
  }
}
