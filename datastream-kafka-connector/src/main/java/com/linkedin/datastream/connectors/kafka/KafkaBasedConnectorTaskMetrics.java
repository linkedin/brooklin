/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;

import com.codahale.metrics.Histogram;
import com.google.common.base.Strings;

import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;

/**
 * Encapsulates common metrics for Kafka-based connectors
 */
public class KafkaBasedConnectorTaskMetrics extends CommonConnectorMetrics {
  // keeps track of paused partitions that are manually paused
  public static final String NUM_CONFIG_PAUSED_PARTITIONS = "numConfigPausedPartitions";
  // keeps track of paused partitions that are auto paused because of error
  public static final String NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR = "numAutoPausedPartitionsOnError";
  // keeps track of paused partitions that are auto paused because of large number of inflight messages
  public static final String NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES =
      "numAutoPausedPartitionsOnInFlightMessages";
  // keeps track of paused partitions that are auto paused because destination does not exist yet
  public static final String NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC =
      "numAutoPausedPartitionsAwaitingDestTopic";
  // keeps track of number of topics that are assigned to the task
  public static final String NUM_TOPICS = "numTopics";
  // keeps track of how long it takes to return from poll()
  public static final String POLL_DURATION_MS = "pollDurationMs";

  private static final Map<String, AtomicLong> AGGREGATED_NUM_TOPICS = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_CONFIG_PAUSED_PARTITIONS = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR =
      new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES =
      new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC =
      new ConcurrentHashMap<>();

  private final AtomicLong _numConfigPausedPartitions = new AtomicLong(0);
  private final AtomicLong _numAutoPausedPartitionsOnError = new AtomicLong(0);
  private final AtomicLong _numAutoPausedPartitionsOnInFlightMessages = new AtomicLong(0);
  private final AtomicLong _numAutoPausedPartitionsAwaitingDestTopic = new AtomicLong(0);
  private final AtomicLong _numTopics = new AtomicLong(0);

  private final Histogram _pollDurationMsMetric;

  KafkaBasedConnectorTaskMetrics(String className, String metricsKey, Logger errorLogger,
      boolean enablePollDurationMillisMetric) {
    super(className, metricsKey, errorLogger);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_CONFIG_PAUSED_PARTITIONS,
        _numConfigPausedPartitions::get);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR,
        _numAutoPausedPartitionsOnError::get);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES,
        _numAutoPausedPartitionsOnInFlightMessages::get);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC,
        _numAutoPausedPartitionsAwaitingDestTopic::get);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_TOPICS, _numTopics::get);

    _pollDurationMsMetric = enablePollDurationMillisMetric ?
        DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, POLL_DURATION_MS, Histogram.class) : null;

    AtomicLong aggNumConfigPausedPartitions =
        AGGREGATED_NUM_CONFIG_PAUSED_PARTITIONS.computeIfAbsent(className, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, AGGREGATE, NUM_CONFIG_PAUSED_PARTITIONS,
        aggNumConfigPausedPartitions::get);
    AtomicLong aggNumAutoPausedPartitionsOnError =
        AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR.computeIfAbsent(className, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, AGGREGATE, NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR,
        aggNumAutoPausedPartitionsOnError::get);
    AtomicLong aggNumAutoPausedPartitionsOnInFlightMessages =
        AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES.computeIfAbsent(className, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, AGGREGATE, NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES,
        aggNumAutoPausedPartitionsOnInFlightMessages::get);
    AtomicLong aggNumAutoPausedPartitionsAwaitingDestTopic =
        AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC.computeIfAbsent(className, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, AGGREGATE, NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC,
        aggNumAutoPausedPartitionsAwaitingDestTopic::get);
    AtomicLong aggNumTopics = AGGREGATED_NUM_TOPICS.computeIfAbsent(className, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, AGGREGATE, NUM_TOPICS, aggNumTopics::get);
  }

  @Override
  public void deregisterMetrics() {
    // this is called when a datastream task is closed/shutdown
    super.deregisterMetrics();
    // update all the aggregates by resetting all of the metrics being de-registered
    updateNumTopics(0);
    updateNumAutoPausedPartitionsAwaitingDestTopic(0);
    updateNumAutoPausedPartitionsOnError(0);
    updateNumConfigPausedPartitions(0);
    updateNumAutoPausedPartitionsOnInFlightMessages(0);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_CONFIG_PAUSED_PARTITIONS);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_TOPICS);

    if (_pollDurationMsMetric != null) {
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, POLL_DURATION_MS);
    }
  }

  /**
   * Set number of config (manually) paused partitions
   * @param val Value to set to
   */
  public void updateNumConfigPausedPartitions(long val) {
    long delta = val - _numConfigPausedPartitions.getAndSet(val);
    AtomicLong aggregatedMetric = AGGREGATED_NUM_CONFIG_PAUSED_PARTITIONS.get(_className);
    if (aggregatedMetric != null) {
      aggregatedMetric.getAndAdd(delta);
    }
  }

  /**
   * Set number of auto paused partitions on error
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsOnError(long val) {
    long delta = val - _numAutoPausedPartitionsOnError.getAndSet(val);
    AtomicLong aggregatedMetric = AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR.get(_className);
    if (aggregatedMetric != null) {
      aggregatedMetric.getAndAdd(delta);
    }
  }

  /**
   * Set number of auto paused partitions on in-flight messages
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsOnInFlightMessages(long val) {
    long delta = val - _numAutoPausedPartitionsOnInFlightMessages.getAndSet(val);
    AtomicLong aggregatedMetric = AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES.get(_className);
    if (aggregatedMetric != null) {
      aggregatedMetric.getAndAdd(delta);
    }
  }

  /**
   * Set number of auto paused partitions awaiting destination topic creation
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsAwaitingDestTopic(long val) {
    long delta = val - _numAutoPausedPartitionsAwaitingDestTopic.getAndSet(val);
    AtomicLong aggregatedMetric = AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC.get(_className);
    if (aggregatedMetric != null) {
      aggregatedMetric.getAndAdd(delta);
    }
  }

  /**
   * Set number of topics
   * @param val Value to set to
   */
  public void updateNumTopics(long val) {
    long delta = val - _numTopics.getAndSet(val);
    AtomicLong aggregatedMetric = AGGREGATED_NUM_TOPICS.get(_className);
    if (aggregatedMetric != null) {
      aggregatedMetric.getAndAdd(delta);
    }
  }

  /**
   * Update the poll duration in millis
   * @param val Value to update
   */
  public void updatePollDurationMs(long val) {
    if (_pollDurationMsMetric != null) {
      _pollDurationMsMetric.update(val);
    }
  }

  /**
   * Utility method for creating task-specific metrics of a Kafka-based connector
   * @param prefix string to prepend to every metric
   */
  public static List<BrooklinMetricInfo> getKafkaBasedConnectorTaskSpecificMetrics(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    prefix = Strings.nullToEmpty(prefix);
    // Specify the attributes to expose to the final metric registry.
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_CONFIG_PAUSED_PARTITIONS));
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR));
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES));
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC));
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_TOPICS));
    metrics.add(new BrooklinHistogramInfo(prefix + POLL_DURATION_MS));
    return Collections.unmodifiableList(metrics);
  }
}
