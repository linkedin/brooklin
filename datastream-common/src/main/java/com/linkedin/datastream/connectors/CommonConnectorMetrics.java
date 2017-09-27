package com.linkedin.datastream.connectors;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.google.common.base.Strings;

import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;


/**
 * Defines a super-set of all possible connector metrics that individual connectors can selectively initialize using
 * the create*Metrics APIs. The individual subset of metrics are grouped by specific activity like event processing
 * or event poll etc. It is up to the connector implementation to instantiate, update and query the right set of
 * metrics. Trying to update a metric from a set that was not instantiated will cause an Exception.
 */
public class CommonConnectorMetrics {
  public static final String AGGREGATE = "aggregate";
  private static final Logger LOG = LoggerFactory.getLogger(CommonConnectorMetrics.class);
  private final Logger _errorLogger;

  // Event processing related metrics
  public static final String EVENTS_PROCESSED_RATE = "eventsProcessedRate";
  public static final String EVENTS_BYTE_PROCESSED_RATE = "eventsByteProcessedRate";
  public static final String ERROR_RATE = "errorRate";
  public static final String NUM_PROCESSING_ABOVE_THRESHOLD = "numProcessingOverThreshold";
  public static final String TIME_SINCE_LAST_EVENT_RECEIVED = "timeSinceLastEventReceivedMs";
  // Per consumer metrics
  private Meter _eventsProcessedRate;
  private Meter _bytesProcessedRate;
  private Meter _errorRate;
  private Meter _numProcessingAboveThreshold;
  private Instant _lastEventReceivedTime;
  // Aggregated metrics
  private static Meter _aggregatedEventsProcessedRate = new Meter();
  private static Meter _aggregatedBytesProcessedRate = new Meter();
  private static Meter _aggregatedErrorRate = new Meter();
  private static Meter _aggregatedProcessingAboveThreshold = new Meter();

  // Poll related metrics
  public static final String NUM_POLLS = "numPolls";
  public static final String EVENT_COUNTS_PER_POLL = "eventCountsPerPoll";
  public static final String CLIENT_POLL_OVER_TIMEOUT = "clientPollOverTimeout";
  public static final String CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT = "pollIntervalOverSessionTimeout";
  // Per consumer metrics
  private Meter _numPolls;
  private Histogram _eventCountsPerPoll;
  // Aggregated metrics
  private static Counter _aggregatedClientPollOverTimeout = new Counter();
  private static Counter _aggregatedClientPollIntervalOverSessionTimeout = new Counter();

  // Partition related metrics
  public static final String REBALANCE_RATE = "rebalanceRate";
  public static final String STUCK_PARTITIONS = "stuckPartitions";
  private final AtomicLong _numStuckPartitions = new AtomicLong(0);
  private static final AtomicLong AGGREGATED_NUM_STUCK_PARTITIONS = new AtomicLong(0);
  // Per consumer metrics
  private Meter _rebalanceRate;
  // Aggregated metrics
  private static Meter _aggregatedRebalanceRate = new Meter();

  private static final DynamicMetricsManager DYNAMIC_METRICS_MANAGER = DynamicMetricsManager.getInstance();

  private final String _className;
  private final String _metricsKey;

  /**
   * Instantiate a ConnectorMetrics instance for a particular Connector implementation
   * @param className The Connector class implementation that is instantiating the metrics class
   * @param metricsKey The key to use with DynamicMetricsManager for creating the full metric names
   *                   DynamicMetricsManager uses both the className and metricsKey is used to construct
   *                   the full metric name.
   */
  public CommonConnectorMetrics(String className, String metricsKey, Logger errorLogger) {
    _errorLogger = errorLogger;
    _className = className;
    _metricsKey = metricsKey;
  }

  /**
   * Create and register the set of metrics related to event processing activity. It is required to call this before an
   * invocation is made to one of updateEventsProcessedRate, updateBytesProcessedRate, updateErrorRate or
   * updateProcessingAboveThreshold
   */
  public void createEventProcessingMetrics() {
    _eventsProcessedRate = new Meter();
    _bytesProcessedRate = new Meter();
    _errorRate = new Meter();
    _numProcessingAboveThreshold = new Meter();
    _lastEventReceivedTime = Instant.now();
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, EVENTS_PROCESSED_RATE, _eventsProcessedRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, EVENTS_BYTE_PROCESSED_RATE, _bytesProcessedRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, ERROR_RATE, _errorRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, NUM_PROCESSING_ABOVE_THRESHOLD,
        _numProcessingAboveThreshold);

    DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, EVENTS_PROCESSED_RATE, _aggregatedEventsProcessedRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, EVENTS_BYTE_PROCESSED_RATE, _aggregatedBytesProcessedRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, ERROR_RATE, _aggregatedErrorRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, NUM_PROCESSING_ABOVE_THRESHOLD,
        _aggregatedProcessingAboveThreshold);

    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, TIME_SINCE_LAST_EVENT_RECEIVED,
        (Gauge<Long>) () -> Duration.between(_lastEventReceivedTime, Instant.now()).toMillis());
  }

  /**
   * Create and register the set of metrics related to event polling activity. It is required to call this before an
   * invocation is made to one of updateNumPolls, updateEventCountsPerPoll, updateClientPollOverTimeout or
   * updateClientPollIntervalOverSessionTimeout
   */
  public void createPollMetrics() {
    _numPolls = new Meter();
    _eventCountsPerPoll = new Histogram(new SlidingTimeWindowReservoir(1, TimeUnit.MINUTES));
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, NUM_POLLS, _numPolls);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, EVENT_COUNTS_PER_POLL, _eventCountsPerPoll);

    DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, CLIENT_POLL_OVER_TIMEOUT,
        _aggregatedClientPollOverTimeout);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT,
        _aggregatedClientPollIntervalOverSessionTimeout);
  }

  /**
   * Create and register the set of metrics related to partition handling activity. It is required to call this before
   * an invocation is made to one of resetStuckPartitions, updateStuckPartitions, updateRebalanceRate
   */
  public void createPartitionMetrics() {
    _rebalanceRate = new Meter();
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, REBALANCE_RATE, _rebalanceRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, _metricsKey, STUCK_PARTITIONS,
        (Gauge<Long>) _numStuckPartitions::get);

    DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, REBALANCE_RATE, _aggregatedRebalanceRate);
    DYNAMIC_METRICS_MANAGER.registerMetric(_className, CommonConnectorMetrics.AGGREGATE, STUCK_PARTITIONS,
        (Gauge<Long>) AGGREGATED_NUM_STUCK_PARTITIONS::get);
  }

  /**
   * Increment the events processed by this consumer and adjust aggregate accordingly
   * @param val Value to increment the metric by
   */
  public void updateEventsProcessedRate(long val) {
      _eventsProcessedRate.mark(val);
      _aggregatedEventsProcessedRate.mark(val);
  }

  /**
   * Increment the event bytes processed by this consumer and adjust aggregate accordingly
   * @param val Value to increment the metric by
   */
  public void updateBytesProcessedRate(long val) {
    _bytesProcessedRate.mark(val);
    _aggregatedBytesProcessedRate.mark(val);
  }

  /**
   * Increment the number of errors seen during event processing by this consumer and adjust aggregate accordingly
   * @param val Value to increment the metric by
   */
  public void updateErrorRate(long val) {
    // TODO: Move logging out of this class; possibly to a common abstract base task handler class in the future
    _errorLogger.error("updateErrorRate with {}. Look for error logs right before this message to see what happened", val);
    updateErrorRate(val, null, null);
  }

  /**
   * Increment the number of errors seen during event processing by this consumer and adjust aggregate accordingly
   * @param val Value to increment the metric by
   * @param message Message associated with the exception
   * @param e Exception that caused the error during event processing
   */
  public void updateErrorRate(long val, String message, Exception e) {
    if (!StringUtils.isEmpty(message)) {
      _errorLogger.error("updateErrorRate with message: " + message, e);
    }
    _errorRate.mark(val);
    _aggregatedErrorRate.mark(val);
  }

  /**
   * Increment the number of events in the batch whose processing was above configured threshold for this consumer
   * and adjust aggregate accordingly
   * @param val Value to increment the metric by
   */
  public void updateProcessingAboveThreshold(long val) {
    _numProcessingAboveThreshold.mark(val);
    _aggregatedProcessingAboveThreshold.mark(val);
  }

  /**
   * Update the time at which the consumer last received an event
   * @param val Time instant to update the metric with
   */
  public void updateLastEventReceivedTime(Instant val) {
    _lastEventReceivedTime = val;
  }

  /**
   * Increment the number of event polls done by this consumer
   * @param val Value to increment the metric by
   */
  public void updateNumPolls(long val) {
    _numPolls.mark(val);
  }

  /**
   * Increment the number of events received by poll by this consumer
   * @param val Value to increment the metric by
   */
  public void updateEventCountsPerPoll(long val) {
    _eventCountsPerPoll.update(val);
  }

  /**
   * Increment the aggregate number of polls that exceeded the specified poll timeout
   * @param val Value to increment metric by
   */
  public void updateClientPollOverTimeout(long val) {
    _aggregatedClientPollOverTimeout.inc(val);
  }

  /**
   * Increment the aggregate number of polls that exceeded the configured session timeout
   * @param val Value to increment metric by
   */
  public void updateClientPollIntervalOverSessionTimeout(long val) {
    _aggregatedClientPollIntervalOverSessionTimeout.inc(val);
  }

  /**
   * Reset the number of stuck partitions on this consumer and adjust the aggregate accordingly
   */
  public void resetStuckPartitions() {
    AGGREGATED_NUM_STUCK_PARTITIONS.getAndAdd(-_numStuckPartitions.getAndSet(0));
  }

  /**
   * Update the number of stuck partitions on this consumer and adjust the aggregate accordingly
   * @param val New value for the metric
   */
  public void updateStuckPartitions(long val) {
    AGGREGATED_NUM_STUCK_PARTITIONS.getAndAdd(val - _numStuckPartitions.getAndSet(val));
  }

  /**
   * Increment the number of rebalances seen by the consumer and adjust aggregate accordingly
   * @param val Value to increment metric by
   */
  public void updateRebalanceRate(long val) {
    _rebalanceRate.mark(val);
    _aggregatedRebalanceRate.mark(val);
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
    return Collections.unmodifiableList(metrics);
  }

  /**
   * Return the list of Brooklin metrics the connector generates for event poll related activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getEventPollMetrics(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    prefix = Strings.nullToEmpty(prefix);
    metrics.add(new BrooklinMeterInfo(prefix + NUM_POLLS));
    metrics.add(new BrooklinCounterInfo(prefix + CLIENT_POLL_OVER_TIMEOUT));
    metrics.add(new BrooklinCounterInfo(prefix + CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT));
    metrics.add(new BrooklinHistogramInfo(prefix + EVENT_COUNTS_PER_POLL));
    return Collections.unmodifiableList(metrics);
  }

  /**
   * Return the list of Brooklin metrics the connector generates for partition based activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getPartitionSpecificMetrics(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    prefix = Strings.nullToEmpty(prefix);
    // Specify the attributes to expose to the final metric registry.
    // TODO: Remove the override once a choice has been made between COUNT and ONE_MINUTE_RATE
    metrics.add(new BrooklinMeterInfo(prefix + REBALANCE_RATE,
        Optional.of(Arrays.asList(BrooklinMeterInfo.COUNT, BrooklinMeterInfo.ONE_MINUTE_RATE))));
    metrics.add(new BrooklinGaugeInfo(prefix + STUCK_PARTITIONS));
    return Collections.unmodifiableList(metrics);
  }
}
