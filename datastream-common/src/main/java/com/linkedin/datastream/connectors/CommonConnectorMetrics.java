package com.linkedin.datastream.connectors;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.google.common.base.Strings;

import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.BrooklinMetrics;
import com.linkedin.datastream.metrics.DynamicMetricsManager;


/**
 * Defines a super-set of all possible connector metrics that individual connectors can selectively initialize using
 * the create*Metrics APIs. The individual subset of metrics are grouped by specific activity like event processing
 * or event poll etc. It is up to the connector implementation to instantiate, update and query the right set of
 * metrics. Trying to update a metric from a set that was not instantiated will cause an Exception.
 */
public class CommonConnectorMetrics {
  public static final String AGGREGATE = "aggregate";
  protected static final DynamicMetricsManager DYNAMIC_METRICS_MANAGER = DynamicMetricsManager.getInstance();

  /**
   * Event processing related metrics
   */
  static class EventProcMetrics extends BrooklinMetrics {
    static final String EVENTS_PROCESSED_RATE = "eventsProcessedRate";
    static final String EVENTS_BYTE_PROCESSED_RATE = "eventsByteProcessedRate";
    static final String ERROR_RATE = "errorRate";
    static final String NUM_PROCESSING_ABOVE_THRESHOLD = "numProcessingOverThreshold";
    static final String TIME_SINCE_LAST_EVENT_RECEIVED = "timeSinceLastEventReceivedMs";

    // Per consumer metrics
    final Meter _eventsProcessedRate;
    final Meter _bytesProcessedRate;
    final Meter _errorRate;
    final Meter _numProcessingAboveThreshold;
    Instant _lastEventReceivedTime;

    // Aggregated metrics
    final Meter _aggregatedEventsProcessedRate;
    final Meter _aggregatedBytesProcessedRate;
    final Meter _aggregatedErrorRate;
    final Meter _aggregatedProcessingAboveThreshold;

    public EventProcMetrics(String className, String key) {
      super(className, key);
      _lastEventReceivedTime = Instant.now();
      _eventsProcessedRate = DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, EVENTS_PROCESSED_RATE,
          Meter.class);
      _bytesProcessedRate = DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, EVENTS_BYTE_PROCESSED_RATE,
          Meter.class);
      _errorRate = DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, ERROR_RATE, Meter.class);
      _numProcessingAboveThreshold = DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key,
          NUM_PROCESSING_ABOVE_THRESHOLD, Meter.class);
      DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, TIME_SINCE_LAST_EVENT_RECEIVED,
          () -> Duration.between(_lastEventReceivedTime, Instant.now()).toMillis());

      // Getting aggregated metrics from DMM, all keyed instances for the same connector share
      // the a single set of aggregated metrics.
      _aggregatedEventsProcessedRate =
          DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, EVENTS_PROCESSED_RATE, Meter.class);
      _aggregatedBytesProcessedRate =
          DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, EVENTS_BYTE_PROCESSED_RATE, Meter.class);
      _aggregatedErrorRate = DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, ERROR_RATE, Meter.class);
      _aggregatedProcessingAboveThreshold =
          DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, NUM_PROCESSING_ABOVE_THRESHOLD, Meter.class);
    }

    @Override
    public void deregister() {
      super.deregister();
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, EVENTS_PROCESSED_RATE);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, EVENTS_BYTE_PROCESSED_RATE);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, ERROR_RATE);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_PROCESSING_ABOVE_THRESHOLD);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, TIME_SINCE_LAST_EVENT_RECEIVED);
    }

    @Override
    protected void deregisterAggregates() {
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, EVENTS_PROCESSED_RATE);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, EVENTS_BYTE_PROCESSED_RATE);
      // Keep aggregate error rate as it is still used for connector error tracking
      // DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, ERROR_RATE);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, NUM_PROCESSING_ABOVE_THRESHOLD);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, TIME_SINCE_LAST_EVENT_RECEIVED);
    }

    // Must be static as MetricInfos are requested from static context
    static List<BrooklinMetricInfo> getMetricInfos(String prefix) {
      List<BrooklinMetricInfo> metrics = new ArrayList<>();
      metrics.add(new BrooklinMeterInfo(prefix + EVENTS_PROCESSED_RATE));
      metrics.add(new BrooklinMeterInfo(prefix + EVENTS_BYTE_PROCESSED_RATE));
      metrics.add(new BrooklinMeterInfo(prefix + ERROR_RATE));
      metrics.add(new BrooklinMeterInfo(prefix + NUM_PROCESSING_ABOVE_THRESHOLD));
      metrics.add(new BrooklinGaugeInfo(prefix + TIME_SINCE_LAST_EVENT_RECEIVED));
      return Collections.unmodifiableList(metrics);
    }
  }

  /**
   * Poll related metrics
   */
  static class PollMetrics extends BrooklinMetrics {
    public static final String NUM_POLLS = "numPolls";
    public static final String EVENT_COUNTS_PER_POLL = "eventCountsPerPoll";
    public static final String CLIENT_POLL_OVER_TIMEOUT = "clientPollOverTimeout";
    public static final String CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT = "pollIntervalOverSessionTimeout";

    // Per consumer metrics
    final Meter _numPolls;
    final Histogram _eventCountsPerPoll;

    // Aggregated metrics
    final Counter _aggregatedClientPollOverTimeout;
    final Counter _aggregatedClientPollIntervalOverSessionTimeout;

    public PollMetrics(String className, String key) {
      super(className, key);
      _numPolls = DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, NUM_POLLS, Meter.class);
      _eventCountsPerPoll = DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, EVENT_COUNTS_PER_POLL,
          Histogram.class);
      // Getting aggregated metrics from DMM, all keyed instances for the same connector share
      // the a single set of aggregated metrics.
      _aggregatedClientPollOverTimeout =
          DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, CLIENT_POLL_OVER_TIMEOUT, Counter.class);
      _aggregatedClientPollIntervalOverSessionTimeout =
          DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT,
              Counter.class);
    }

    @Override
    public void deregister() {
      super.deregister();
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_POLLS);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, EVENT_COUNTS_PER_POLL);
    }

    @Override
    protected void deregisterAggregates() {
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, CLIENT_POLL_OVER_TIMEOUT);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT);
    }

    // Must be static as MetricInfos are requested from static context
    static List<BrooklinMetricInfo> getMetricInfos(String prefix) {
      List<BrooklinMetricInfo> metrics = new ArrayList<>();
      metrics.add(new BrooklinMeterInfo(prefix + NUM_POLLS));
      metrics.add(new BrooklinCounterInfo(prefix + CLIENT_POLL_OVER_TIMEOUT));
      metrics.add(new BrooklinCounterInfo(prefix + CLIENT_POLL_INTERVAL_OVER_SESSION_TIMEOUT));
      metrics.add(new BrooklinHistogramInfo(prefix + EVENT_COUNTS_PER_POLL));
      return Collections.unmodifiableList(metrics);
    }
  }

  /**
   * Partition related metrics
   */
  static class PartitionMetrics extends BrooklinMetrics {
    // Partition related metrics
    static final String REBALANCE_RATE = "rebalanceRate";
    static final String STUCK_PARTITIONS = "stuckPartitions";

    // Per consumer metrics
    final AtomicLong _numStuckPartitions = new AtomicLong(0);
    final Meter _rebalanceRate;

    // Aggregated metrics
    final Meter _aggregatedRebalanceRate;

    // Map from connector class name to its stuck partition counter
    // This is needed for Gauge metrics which need long-typed suppliers.
    static final Map<String, AtomicLong> AGGREGATED_NUM_STUCK_PARTITIONS = new ConcurrentHashMap<>();

    public PartitionMetrics(String className, String key) {
      super(className, key);
      _rebalanceRate = DYNAMIC_METRICS_MANAGER.registerMetric(_className, _key, REBALANCE_RATE, Meter.class);
      DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, STUCK_PARTITIONS, _numStuckPartitions::get);

      _aggregatedRebalanceRate = DYNAMIC_METRICS_MANAGER.registerMetric(_className, AGGREGATE, REBALANCE_RATE, Meter.class);

      AtomicLong aggStuckPartitions = AGGREGATED_NUM_STUCK_PARTITIONS.computeIfAbsent(className, k -> new AtomicLong(0));
      DYNAMIC_METRICS_MANAGER.registerGauge(_className, AGGREGATE, STUCK_PARTITIONS, () -> aggStuckPartitions.get());
    }

    @Override
    public void deregister() {
      super.deregister();
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, REBALANCE_RATE);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, STUCK_PARTITIONS);
    }

    @Override
    protected void deregisterAggregates() {
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, REBALANCE_RATE);
      DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, AGGREGATE, STUCK_PARTITIONS);
      AGGREGATED_NUM_STUCK_PARTITIONS.remove(_className);
    }

    public void resetStuckPartitions() {
      long numStuckPartitions = _numStuckPartitions.getAndSet(0);
      AGGREGATED_NUM_STUCK_PARTITIONS.get(_className).getAndAdd(-numStuckPartitions);
    }

    public void updateStuckPartitions(long val) {
      long delta = val - _numStuckPartitions.getAndSet(val);
      AGGREGATED_NUM_STUCK_PARTITIONS.get(_className).getAndAdd(delta);
    }

    // Must be static as MetricInfos are requested from static context
    static List<BrooklinMetricInfo> getMetricInfos(String prefix) {
      List<BrooklinMetricInfo> metrics = new ArrayList<>();
      // Specify the attributes to expose to the final metric registry.
      // TODO: Remove the override once a choice has been made between COUNT and ONE_MINUTE_RATE
      metrics.add(new BrooklinMeterInfo(prefix + REBALANCE_RATE,
          Optional.of(Arrays.asList(BrooklinMeterInfo.COUNT, BrooklinMeterInfo.ONE_MINUTE_RATE))));
      metrics.add(new BrooklinGaugeInfo(prefix + STUCK_PARTITIONS));
      return Collections.unmodifiableList(metrics);
    }
  }

  protected final String _className;
  protected final String _key;
  protected final Logger _errorLogger;
  private final List<BrooklinMetrics> _metricsList = new ArrayList<>();

  private EventProcMetrics _eventProcMetrics;
  private PollMetrics _pollMetrics;
  private PartitionMetrics _partitionMetrics;

  /**
   * Instantiate a ConnectorMetrics instance for a particular Connector implementation
   * @param className The Connector class implementation that is instantiating the metrics class
   * @param key The key to use with DynamicMetricsManager for creating the full metric names
   *                   DynamicMetricsManager uses both the className and key is used to construct
   *                   the full metric name.
   */
  public CommonConnectorMetrics(String className, String key, Logger errorLogger) {
    Validate.notNull(className, "className cannot be null.");
    _className = className;
    _key = key;
    _errorLogger = errorLogger;
  }

  /**
   * Create and register the set of metrics related to event processing activity. It is required to call this before an
   * invocation is made to one of updateEventsProcessedRate, updateBytesProcessedRate, updateErrorRate or
   * updateProcessingAboveThreshold
   */
  public void createEventProcessingMetrics() {
    _eventProcMetrics = new EventProcMetrics(_className, _key);
    _metricsList.add(_eventProcMetrics);
  }

  /**
   * Create and register the set of metrics related to event polling activity. It is required to call this before an
   * invocation is made to one of updateNumPolls, updateEventCountsPerPoll, updateClientPollOverTimeout or
   * updateClientPollIntervalOverSessionTimeout
   */
  public void createPollMetrics() {
    _pollMetrics = new PollMetrics(_className, _key);
    _metricsList.add(_pollMetrics);
  }

  /**
   * Create and register the set of metrics related to partition handling activity. It is required to call this before
   * an invocation is made to one of resetStuckPartitions, updateStuckPartitions, updateRebalanceRate
   */
  public void createPartitionMetrics() {
    _partitionMetrics = new PartitionMetrics(_className, _key);
    _metricsList.add(_partitionMetrics);
  }

  /**
   * Deregister all currently registered metrics from DynamicMetricsManager. This should be called when
   * the connector does not want to publish metric values any more because of circumstances like task
   * unassignments, etc.
   */
  public void deregisterMetrics() {
    _metricsList.stream().forEach(m -> m.deregister());
  }

  /**
   * Increment the events processed by this consumer and adjust aggregate accordingly
   * @param val Value to increment the metric by
   */
  public void updateEventsProcessedRate(long val) {
    _eventProcMetrics._eventsProcessedRate.mark(val);
    _eventProcMetrics._aggregatedEventsProcessedRate.mark(val);
  }

  /**
   * Increment the event bytes processed by this consumer and adjust aggregate accordingly
   * @param val Value to increment the metric by
   */
  public void updateBytesProcessedRate(long val) {
    _eventProcMetrics._bytesProcessedRate.mark(val);
    _eventProcMetrics._aggregatedBytesProcessedRate.mark(val);
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
    _eventProcMetrics._errorRate.mark(val);
    _eventProcMetrics._aggregatedErrorRate.mark(val);
  }

  /**
   * Increment the number of errors (aggregated only) which are global and not tied to any particular scope.
   * @param val Value to increment the metric by
   * @param message Message associated with the exception
   * @param e Exception that caused the error during event processing
   */
  public void updateAggregatedErrorRate(long val, String message, Exception e) {
    if (!StringUtils.isEmpty(message)) {
      _errorLogger.error("updateErrorRate with message: " + message, e);
    }
    _eventProcMetrics._aggregatedErrorRate.mark(val);
  }

  /**
   * Increment the number of events in the batch whose processing was above configured threshold for this consumer
   * and adjust aggregate accordingly
   * @param val Value to increment the metric by
   */
  public void updateProcessingAboveThreshold(long val) {
    _eventProcMetrics._numProcessingAboveThreshold.mark(val);
    _eventProcMetrics._aggregatedProcessingAboveThreshold.mark(val);
  }

  /**
   * Update the time at which the consumer last received an event
   * @param val Time instant to update the metric with
   */
  public void updateLastEventReceivedTime(Instant val) {
    _eventProcMetrics._lastEventReceivedTime = val;
  }

  /**
   * Increment the number of event polls done by this consumer
   * @param val Value to increment the metric by
   */
  public void updateNumPolls(long val) {
    _pollMetrics._numPolls.mark(val);
  }

  /**
   * Increment the number of events received by poll by this consumer
   * @param val Value to increment the metric by
   */
  public void updateEventCountsPerPoll(long val) {
    _pollMetrics._eventCountsPerPoll.update(val);
  }

  /**
   * Increment the aggregate number of polls that exceeded the specified poll timeout
   * @param val Value to increment metric by
   */
  public void updateClientPollOverTimeout(long val) {
    _pollMetrics._aggregatedClientPollOverTimeout.inc(val);
  }

  /**
   * Increment the aggregate number of polls that exceeded the configured session timeout
   * @param val Value to increment metric by
   */
  public void updateClientPollIntervalOverSessionTimeout(long val) {
    _pollMetrics._aggregatedClientPollIntervalOverSessionTimeout.inc(val);
  }

  /**
   * Reset the number of stuck partitions on this consumer and adjust the aggregate accordingly
   */
  public void resetStuckPartitions() {
    _partitionMetrics.resetStuckPartitions();
  }

  /**
   * Update the number of stuck partitions on this consumer and adjust the aggregate accordingly
   * @param val New value for the metric
   */
  public void updateStuckPartitions(long val) {
    _partitionMetrics.updateStuckPartitions(val);
  }

  /**
   * Increment the number of rebalances seen by the consumer and adjust aggregate accordingly
   * @param val Value to increment metric by
   */
  public void updateRebalanceRate(long val) {
    _partitionMetrics._rebalanceRate.mark(val);
    _partitionMetrics._aggregatedRebalanceRate.mark(val);
  }

  /**
   * Return the list of Brooklin metrics the connector generates for event processing related activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getEventProcessingMetrics(String prefix) {
    prefix = Strings.nullToEmpty(prefix);
    return EventProcMetrics.getMetricInfos(prefix);
  }

  /**
   * Return the list of Brooklin metrics the connector generates for event poll related activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getEventPollMetrics(String prefix) {
    prefix = Strings.nullToEmpty(prefix);
    return PollMetrics.getMetricInfos(prefix);
  }

  /**
   * Return the list of Brooklin metrics the connector generates for partition based activity
   * @param prefix Prefix to append to create the BrooklinMetricInfo.
   * @return List of exposed Brooklin metrics for the connector activity
   */
  public static List<BrooklinMetricInfo> getPartitionSpecificMetrics(String prefix) {
    prefix = Strings.nullToEmpty(prefix);
    return PartitionMetrics.getMetricInfos(prefix);
  }
}
