package com.linkedin.datastream.connectors.kafka;

import com.google.common.base.Strings;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;


public class KafkaBasedConnectorTaskMetrics extends CommonConnectorMetrics {
  // keeps track of paused partitions that are manually paused
  public static final String NUM_CONFIG_PAUSED_PARTITIONS = "numConfigPausedPartitions";
  // keeps track of paused partitions that are auto paused because of error
  public static final String NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR = "numAutoPausedPartitionsOnError";
  // keeps track of paused partitions that are auto paused because of large number of inflight messages
  public static final String NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES = "numAutoPausedPartitionsOnInFlightMessages";

  final AtomicLong _numConfigPausedPartitions = new AtomicLong(0);
  final AtomicLong _numAutoPausedPartitionsOnError = new AtomicLong(0);
  final AtomicLong _numAutoPausedPartitionsOnInFlightMessages = new AtomicLong(0);


  KafkaBasedConnectorTaskMetrics(String className, String metricsKey, Logger errorLogger) {
    super(className, metricsKey, errorLogger);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_CONFIG_PAUSED_PARTITIONS, _numConfigPausedPartitions::get);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR, _numAutoPausedPartitionsOnError::get);
    DYNAMIC_METRICS_MANAGER.registerGauge(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES, _numAutoPausedPartitionsOnInFlightMessages::get);
  }

  @Override
  public void deregisterMetrics() {
    super.deregisterMetrics();
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_CONFIG_PAUSED_PARTITIONS);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR);
    DYNAMIC_METRICS_MANAGER.unregisterMetric(_className, _key, NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES);
  }

  public static List<BrooklinMetricInfo> getKafkaBasedConnectorTaskSpecificMetrics(String prefix) {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    prefix = Strings.nullToEmpty(prefix);
    // Specify the attributes to expose to the final metric registry.
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_CONFIG_PAUSED_PARTITIONS));
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR));
    metrics.add(new BrooklinGaugeInfo(prefix + NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES));
    return Collections.unmodifiableList(metrics);
  }

  /**
   * Set number of config (manually) paused partitions
   * @param val Value to set to
   */
  public void updateNumConfigPausedPartitions(long val) {
    _numConfigPausedPartitions.set(val);
  }

  /**
   * Set number of auto paused partitions
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsOnError(long val) {
    _numAutoPausedPartitionsOnError.set(val);
  }

  /**
   * Set number of auto paused partitions
   * @param val Value to set to
   */
  public void updateNumAutoPausedPartitionsOnInFlightMessages(long val) {
    _numAutoPausedPartitionsOnInFlightMessages.set(val);
  }
}
