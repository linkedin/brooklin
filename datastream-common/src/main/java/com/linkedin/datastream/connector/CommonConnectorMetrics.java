package com.linkedin.datastream.connector;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.common.base.Strings;


/**
 * Defines a super-set of all possible connector metrics that individual connectors can selectively initialize using
 * the createEvent*Metrics APIs. The individual subset of metrics are grouped by specific activity like event processing
 * or event poll etc. It is up to the connector implementation to query instantiate, update and query the right set of
 * metrics. Updating a metric from a set that was not instantiated might still end up registering the metric with the
 * metrics registry.
 */
public class CommonConnectorMetrics {
  public static final String AGGREGATE = "aggregate";
  public static final Logger LOG = LoggerFactory.getLogger(CommonConnectorMetrics.class);

  // Event processing related metrics
  public static final String EVENTS_PROCESSED_RATE = "eventsProcessedRate";
  public static final String EVENTS_BYTE_PROCESSED_RATE = "eventsByteProcessedRate";
  public static final String ERROR_RATE = "errorRate";
  public static final String NUM_PROCESSING_ABOVE_THRESHOLD = "numProcessingOverThreshold";
  public static final String TIME_SINCE_LAST_EVENT_RECEIVED = "timeSinceLastEventReceivedMs";

  // Poll related metrics
  static final String NUM_KAFKA_POLLS = "numKafkaPolls";
  static final String EVENT_COUNTS_PER_POLL = "eventCountsPerPoll";
  static final String CLIENT_POLL_OVER_TIMEOUT = "clientPollOverTimeout";
  static final String CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT = "pollIntervalOverSessionTimeout";
  private Instant _lastEventReceivedTime = Instant.now();

  // Partition related metrics
  static final String REBALANCE_RATE = "rebalanceRate";
  static final String STUCK_PARTITIONS = "stuckPartitions";
  private final AtomicLong _numStuckPartitions = new AtomicLong(0);
  private static final AtomicLong AGGREGATED_NUM_STUCK_PARTITIONS = new AtomicLong(0);

  private static final DynamicMetricsManager DYNAMIC_METRICS_MANAGER = DynamicMetricsManager.getInstance();

  private final String _className;
  private final String _metricsKey;

  public CommonConnectorMetrics(String className, String metricsKey) {
    _className = className;
    _metricsKey = metricsKey;
  }

  public void createEventProcessingMetrics() {
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, TIME_SINCE_LAST_EVENT_RECEIVED,
        (Gauge<Long>) () -> Duration.between(_lastEventReceivedTime, Instant.now()).toMillis());
  }

  public void createPollMetrics() {
    DYNAMIC_METRICS_MANAGER.createHistogram(_className, _metricsKey, EVENT_COUNTS_PER_POLL,
        new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES));
  }

  public void createPartitionMetrics() {
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, STUCK_PARTITIONS,
        (Gauge<Long>) _numStuckPartitions::get);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, CommonConnectorMetrics.AGGREGATE, STUCK_PARTITIONS,
        (Gauge<Long>) AGGREGATED_NUM_STUCK_PARTITIONS::get);
  }

  public void updateProcessingAboveThreshold(long val) {
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, _metricsKey, NUM_PROCESSING_ABOVE_THRESHOLD, val);
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, CommonConnectorMetrics.AGGREGATE, NUM_PROCESSING_ABOVE_THRESHOLD, val);
  }

  public void updateErrorRate(long val) {
    // TODO: Logs should be per connector choice right?
    // LOG.error("updateErrorRate with {}. Look for error logs right before this message to see what happened", val);
    updateErrorRate(val, null, null);
  }

  public void updateErrorRate(long val, String message, Exception e) {
    if (!StringUtils.isEmpty(message)) {
      LOG.error("updateErrorRate with message: " + message, e);
    }
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, _metricsKey, ERROR_RATE, val);
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, CommonConnectorMetrics.AGGREGATE, ERROR_RATE, val);
  }

  public void updateEventsProcessedRate(long val) {
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, _metricsKey, EVENTS_PROCESSED_RATE, val);
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, CommonConnectorMetrics.AGGREGATE, EVENTS_PROCESSED_RATE, val);
  }

  public void updateBytesProcessedRate(long val) {
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, _metricsKey, EVENTS_BYTE_PROCESSED_RATE, val);
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, CommonConnectorMetrics.AGGREGATE, EVENTS_BYTE_PROCESSED_RATE, val);
  }

  public void updateLastEventReceivedTime(Instant val) {
    _lastEventReceivedTime = val;
  }

  public void updateNumKafkaPolls(long val) {
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, _metricsKey, NUM_KAFKA_POLLS, val);
  }

  public void updateEventCountsPerPoll(long val) {
    DYNAMIC_METRICS_MANAGER.updateHistogram(_className, _metricsKey, EVENT_COUNTS_PER_POLL, val);
  }

  public void updateClientPollOverTimeout(long val) {
    DYNAMIC_METRICS_MANAGER.createOrUpdateCounter(_className, CommonConnectorMetrics.AGGREGATE, CLIENT_POLL_OVER_TIMEOUT, val);
  }

  public void updateClientPollIntervalOverSessionTimeout(long val) {
    DYNAMIC_METRICS_MANAGER.createOrUpdateCounter(_className, CommonConnectorMetrics.AGGREGATE, CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT, val);
  }

  public void resetStuckPartitions() {
    AGGREGATED_NUM_STUCK_PARTITIONS.getAndAdd(-_numStuckPartitions.getAndSet(0));
  }

  public void updateStuckPartitions(long val) {
    AGGREGATED_NUM_STUCK_PARTITIONS.getAndAdd(val - _numStuckPartitions.getAndSet(val));
  }

  public void updateRebalanceRate(long val) {
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, _metricsKey, REBALANCE_RATE, val);
    DYNAMIC_METRICS_MANAGER.createOrUpdateMeter(_className, CommonConnectorMetrics.AGGREGATE,
        REBALANCE_RATE, val);
  }

  /**
   * Return the list of Brooklin metrics the connector generates for event processing related activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getEventProcessingMetrics(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    prefix = Strings.nullToEmpty(prefix);
    metrics.add(new BrooklinMeterInfo(prefix + EVENTS_PROCESSED_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + EVENTS_BYTE_PROCESSED_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + ERROR_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + NUM_PROCESSING_ABOVE_THRESHOLD));
    metrics.add(new BrooklinGaugeInfo(prefix + TIME_SINCE_LAST_EVENT_RECEIVED));
    return metrics;
  }

  /**
   * Return the list of Brooklin metrics the connector generates for event poll related activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getEventPollMetrics(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    prefix = Strings.nullToEmpty(prefix);
    metrics.add(new BrooklinMeterInfo(prefix + NUM_KAFKA_POLLS));
    metrics.add(new BrooklinCounterInfo(prefix + CLIENT_POLL_OVER_TIMEOUT));
    metrics.add(new BrooklinCounterInfo(prefix + CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT));
    metrics.add(new BrooklinHistogramInfo(prefix + EVENT_COUNTS_PER_POLL));
    return metrics;
  }

  /**
   * Return the list of Brooklin metrics the connector generates for partition based activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getPartitionSpecificMetrics(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    prefix = Strings.nullToEmpty(prefix);
    metrics.add(new BrooklinMeterInfo(prefix + REBALANCE_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + STUCK_PARTITIONS));
    return metrics;
  }
}
