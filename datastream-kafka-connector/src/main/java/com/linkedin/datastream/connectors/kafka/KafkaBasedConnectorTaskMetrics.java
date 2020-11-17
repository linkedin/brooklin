/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.slf4j.Logger;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;
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
  // keeps track of how long processing takes between polls
  public static final String TIME_SPENT_BETWEEN_POLLS_MS = "timeSpentBetweenPollsMs";
  // keeps track of process + send time per event returned from poll() in nanoseconds
  public static final String PER_EVENT_PROCESSING_TIME_NANOS = "perEventProcessingTimeNs";
  // keeps track of the sum of how far the safe offset is behind the watermark for all TopicPartitions
  public static final String CONSUMER_OFFSET_WATERMARK_SPAN = "consumerOffsetWatermarkSpan";
  // keeps track of how many times the consumer had OffsetOutOfRangeException or NoOffsetForPartitionException
  public static final String CONSUMER_LICLOSEST_DATA_LOSS_ESTIMATION = "consumerLiclosestDataLossEstimation";

  private static final Map<String, AtomicLong> AGGREGATED_NUM_TOPICS = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_CONFIG_PAUSED_PARTITIONS = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR =
      new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES =
      new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC =
      new ConcurrentHashMap<>();

  private static final Map<String, AtomicLong> NUM_TOPICS_PER_METRIC_KEY = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> NUM_CONFIG_PAUSED_PARTITIONS_PER_METRIC_KEY = new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR_PER_METRIC_KEY =
      new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES_PER_METRIC_KEY =
      new ConcurrentHashMap<>();
  private static final Map<String, AtomicLong> NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC_PER_METRIC_KEY =
      new ConcurrentHashMap<>();

  private final AtomicLong _numConfigPausedPartitions;
  private final AtomicLong _numAutoPausedPartitionsOnError;
  private final AtomicLong _numAutoPausedPartitionsOnInFlightMessages;
  private final AtomicLong _numAutoPausedPartitionsAwaitingDestTopic;
  private final AtomicLong _numTopics;

  private final Histogram _pollDurationMsMetric;
  private final Histogram _timeSpentBetweenPollsMsMetric;
  private final Histogram _perEventProcessingTimeNanosMetric;
  private final String _fullMetricsKey;

  KafkaBasedConnectorTaskMetrics(String className, String metricsKey, Logger errorLogger,
      boolean enableAdditionalMetrics) {
    super(className, metricsKey, errorLogger);
    _fullMetricsKey = MetricRegistry.name(_className, _key);
    _numConfigPausedPartitions = new AtomicLong(0);
    _numAutoPausedPartitionsOnError = new AtomicLong(0);
    _numAutoPausedPartitionsOnInFlightMessages = new AtomicLong(0);
    _numAutoPausedPartitionsAwaitingDestTopic = new AtomicLong(0);
    _numTopics = new AtomicLong(0);
    AtomicLong numConfigPausedPartitions =
        NUM_CONFIG_PAUSED_PARTITIONS_PER_METRIC_KEY.computeIfAbsent(_fullMetricsKey, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_CONFIG_PAUSED_PARTITIONS,
        numConfigPausedPartitions::get);
    AtomicLong numAutoPausedPartitionsOnError =
        NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR_PER_METRIC_KEY.computeIfAbsent(_fullMetricsKey, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR,
        numAutoPausedPartitionsOnError::get);
    AtomicLong numAutoPausedPartitionsOnInFlightMessages =
        NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES_PER_METRIC_KEY.computeIfAbsent(_fullMetricsKey, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES,
        numAutoPausedPartitionsOnInFlightMessages::get);
    AtomicLong numAutoPausedPartitionsAwaitingDestTopic =
        NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC_PER_METRIC_KEY.computeIfAbsent(_fullMetricsKey, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, AGGREGATE, NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC,
        numAutoPausedPartitionsAwaitingDestTopic::get);
    AtomicLong numTopics = NUM_TOPICS_PER_METRIC_KEY.computeIfAbsent(_fullMetricsKey, k -> new AtomicLong(0));
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_TOPICS, numTopics::get);

    _pollDurationMsMetric = enableAdditionalMetrics ?
        DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, POLL_DURATION_MS, Histogram.class) : null;
    _timeSpentBetweenPollsMsMetric = enableAdditionalMetrics ?
        DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, TIME_SPENT_BETWEEN_POLLS_MS, Histogram.class) : null;
    _perEventProcessingTimeNanosMetric = enableAdditionalMetrics ?
        DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, PER_EVENT_PROCESSING_TIME_NANOS, Histogram.class) : null;

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

  /**
   * Register some of the Kafka consumer metrics of interest. This cannot be done as part of the constructor
   * as the Kafka consumer may be created at a later time than this object.
   * @param consumer the Kafka consumer for which to register the metrics
   * @param clientId the Kafka consumer's client.id
   */
  public void registerKafkaConsumerMetrics(Consumer<?, ?> consumer, String clientId) {
    if (StringUtils.isBlank(clientId)) {
      _errorLogger.warn("Cannot register the Kafka consumer metrics, the client.id is blank");
      return;
    }

    Supplier<Double> watermarkSpanSupplier = () -> getConsumerOffsetWatermarkSpanMetric(consumer, clientId);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, CONSUMER_OFFSET_WATERMARK_SPAN, watermarkSpanSupplier);

    Supplier<Double> offsetResetSupplier = () -> getConsumerLiclosestDataLossEstimationMetric(consumer, clientId);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, CONSUMER_LICLOSEST_DATA_LOSS_ESTIMATION, offsetResetSupplier);
  }

  private double getConsumerOffsetWatermarkSpanMetric(Consumer<?, ?> consumer, String clientId) {
    return getConsumerMetricValue(consumer, clientId, "consumer-offset-watermark-span");
  }

  private double getConsumerLiclosestDataLossEstimationMetric(Consumer<?, ?> consumer, String clientId) {
    return getConsumerMetricValue(consumer, clientId, "consumer-liclosest-data-loss-estimation");
  }

  private double getConsumerMetricValue(Consumer<?, ?> consumer, String clientId, String metricName) {
    Map<String, String> tags = new HashMap<>(2);
    tags.put("client-id", clientId);
    MetricName name = new MetricName(metricName, "lnkd", "", tags);
    Metric metric = consumer.metrics().get(name);
    return (metric != null && metric.metricValue() instanceof Double) ? (Double) metric.metricValue() : 0.0;
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
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, CONSUMER_OFFSET_WATERMARK_SPAN);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, CONSUMER_LICLOSEST_DATA_LOSS_ESTIMATION);

    if (_pollDurationMsMetric != null) {
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, POLL_DURATION_MS);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, TIME_SPENT_BETWEEN_POLLS_MS);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, PER_EVENT_PROCESSING_TIME_NANOS);
    }
  }

  /**
   * Set number of config (manually) paused partitions
   * @param val Value to set to
   */
  public void updateNumConfigPausedPartitions(long val) {
    long delta = val - _numConfigPausedPartitions.getAndSet(val);
    updateMetrics(delta, NUM_CONFIG_PAUSED_PARTITIONS_PER_METRIC_KEY, AGGREGATED_NUM_CONFIG_PAUSED_PARTITIONS);
  }

  /**
   * Set number of auto paused partitions on error
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsOnError(long val) {
    long delta = val - _numAutoPausedPartitionsOnError.getAndSet(val);
    updateMetrics(delta, NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR_PER_METRIC_KEY,
        AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR);
  }

  /**
   * Set number of auto paused partitions on in-flight messages
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsOnInFlightMessages(long val) {
    long delta = val - _numAutoPausedPartitionsOnInFlightMessages.getAndSet(val);
    updateMetrics(delta, NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES_PER_METRIC_KEY,
        AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES);
  }

  /**
   * Set number of auto paused partitions awaiting destination topic creation
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsAwaitingDestTopic(long val) {
    long delta = val - _numAutoPausedPartitionsAwaitingDestTopic.getAndSet(val);
    updateMetrics(delta, NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC_PER_METRIC_KEY,
        AGGREGATED_NUM_AUTO_PAUSED_PARTITIONS_WAITING_FOR_DEST_TOPIC);
  }

  /**
   * Set number of topics
   * @param val Value to set to
   */
  public void updateNumTopics(long val) {
    long delta = val - _numTopics.getAndSet(val);
    updateMetrics(delta, NUM_TOPICS_PER_METRIC_KEY, AGGREGATED_NUM_TOPICS);
  }

  private void updateMetrics(long val, Map<String, AtomicLong> metricsMap,
      Map<String, AtomicLong> aggregatedMetricsMap) {

    AtomicLong metric = metricsMap.get(_fullMetricsKey);
    if (metric != null) {
      metric.getAndAdd(val);
    }
    AtomicLong aggregatedMetric = aggregatedMetricsMap.get(_className);
    if (aggregatedMetric != null) {
      aggregatedMetric.getAndAdd(val);
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
   * Update the time spent between polls in millis
   * @param val Value to update
   */
  public void updateTimeSpentBetweenPollsMs(long val) {
    if (_timeSpentBetweenPollsMsMetric != null) {
      _timeSpentBetweenPollsMsMetric.update(val);
    }
  }

  /**
   * Update the event processing time in nanos
   * @param val Value to update
   */
  public void updatePerEventProcessingTimeNanos(long val) {
    if (_perEventProcessingTimeNanosMetric != null) {
      _perEventProcessingTimeNanosMetric.update(val);
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
    metrics.add(new BrooklinGaugeInfo(prefix + CONSUMER_OFFSET_WATERMARK_SPAN));
    metrics.add(new BrooklinGaugeInfo(prefix + CONSUMER_LICLOSEST_DATA_LOSS_ESTIMATION));
    metrics.add(new BrooklinHistogramInfo(prefix + POLL_DURATION_MS));
    metrics.add(new BrooklinHistogramInfo(prefix + TIME_SPENT_BETWEEN_POLLS_MS));
    metrics.add(new BrooklinHistogramInfo(prefix + PER_EVENT_PROCESSING_TIME_NANOS));
    return Collections.unmodifiableList(metrics);
  }
}
