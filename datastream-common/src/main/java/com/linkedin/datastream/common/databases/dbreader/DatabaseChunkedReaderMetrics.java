package com.linkedin.datastream.common.databases.dbreader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;

import com.linkedin.datastream.metrics.BrooklinHistogramInfo;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.BrooklinMetrics;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;


/**
 * Metrics for the DatabaseChunkedReader.
 * There are 2 levels of metrics. Per reader metrics, per source-aggregate metrics.
 */
class DatabaseChunkedReaderMetrics extends BrooklinMetrics {
  private static final String CLASS_NAME = DatabaseChunkedReader.class.getSimpleName();
  private static final String SOURCE_METRICS_PREFIX_REGEX = CLASS_NAME + MetricsAware.KEY_REGEX;

  private static final String QUERY_EXECUTION_TIME = "QueryExecutionTimeMs";
  private static final String QUERY_EXECUTION_RATE = "QueryExecutionRate";
  private static final String ERROR_RATE = "errorRate";
  public static final String SKIPPED_BAD_MESSAGES_RATE = "skippedBadMessagesRate";

  // Per reader metrics
  private Histogram _readerQueryExecutionTimeMs;
  private Meter _readerQueriesExecutedRate;
  private Meter _readerErrorRate;
  private Meter _readerSkippedBadMessagesRate;

  // Per source aggregated metrics
  private Histogram _sourceQueryExecutionTimeMs;
  private Meter _sourceQueriesExecutedRate;
  private Meter _sourceErrorRate;
  private Meter _sourceSkippedBadMessagesRate;

  private String _source;

  protected static final DynamicMetricsManager DYNAMIC_METRICS_MANAGER = DynamicMetricsManager.getInstance();

  public DatabaseChunkedReaderMetrics(String source, String key) {
    super(CLASS_NAME, key);
    _source = source;

    _readerQueryExecutionTimeMs = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, key, QUERY_EXECUTION_TIME, Histogram.class);
    _readerQueriesExecutedRate = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, key, QUERY_EXECUTION_RATE, Meter.class);
    _readerErrorRate = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, key, ERROR_RATE, Meter.class);
    _readerSkippedBadMessagesRate = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, key, SKIPPED_BAD_MESSAGES_RATE, Meter.class);

    _sourceQueryExecutionTimeMs = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, source, QUERY_EXECUTION_TIME, Histogram.class);
    _sourceQueriesExecutedRate = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, source, QUERY_EXECUTION_RATE, Meter.class);
    _sourceErrorRate = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, source, ERROR_RATE, Meter.class);
    _sourceSkippedBadMessagesRate = DYNAMIC_METRICS_MANAGER.registerMetric(CLASS_NAME, source, SKIPPED_BAD_MESSAGES_RATE, Meter.class);
  }

  @Override
  public void deregister() {
    super.deregister();
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, QUERY_EXECUTION_TIME);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, QUERY_EXECUTION_RATE);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, ERROR_RATE);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, SKIPPED_BAD_MESSAGES_RATE);
  }

  @Override
  protected void deregisterAggregates() {
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _source, QUERY_EXECUTION_TIME);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _source, QUERY_EXECUTION_RATE);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _source, ERROR_RATE);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _source, SKIPPED_BAD_MESSAGES_RATE);
  }

  static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.add(new BrooklinHistogramInfo(SOURCE_METRICS_PREFIX_REGEX + QUERY_EXECUTION_TIME));
    metrics.add(new BrooklinMeterInfo(SOURCE_METRICS_PREFIX_REGEX + QUERY_EXECUTION_RATE));
    metrics.add(new BrooklinMeterInfo(SOURCE_METRICS_PREFIX_REGEX + ERROR_RATE));
    metrics.add(new BrooklinMeterInfo(SOURCE_METRICS_PREFIX_REGEX + SKIPPED_BAD_MESSAGES_RATE));

    return Collections.unmodifiableList(metrics);
  }

  void updateQueryExecutionTime(long executionTimeMs) {
    _readerQueryExecutionTimeMs.update(executionTimeMs);
    _sourceQueryExecutionTimeMs.update(executionTimeMs);
  }

  void updateQueryExecutionRate(int count) {
    _readerQueriesExecutedRate.mark(count);
    _sourceQueriesExecutedRate.mark(count);
  }

  void updateQueryExecutionRate() {
    updateQueryExecutionRate(1);
  }

  void updateErrorRate() {
    _readerErrorRate.mark();
    _sourceErrorRate.mark();
  }

  void updateSkipBadMessagesRate(int count) {
    _readerSkippedBadMessagesRate.mark(count);
    _sourceSkippedBadMessagesRate.mark(count);
  }

  void updateSkipBadMessagesRate() {
    updateSkipBadMessagesRate(1);
  }
}
