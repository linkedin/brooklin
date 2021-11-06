/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.time.Duration;
import java.util.Properties;

import com.linkedin.datastream.common.VerifiableProperties;

/**
 * Configuration properties for {@link LoadBasedPartitionAssignmentStrategy} and its extensions
 */
public class LoadBasedPartitionAssignmentStrategyConfig extends PartitionAssignmentStrategyConfig {

  public static final String CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS = "throughputInfoFetchTimeoutMs";
  public static final String CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS = "throughputInfoFetchRetryPeriodMs";
  public static final String CFG_TASK_CAPACITY_MBPS = "taskCapacityMBps";
  public static final String CFG_TASK_CAPACITY_UTILIZATION_PCT = "taskCapacityUtilizationPct";
  public static final String CFG_ENABLE_THROUGHPUT_BASED_PARTITION_ASSIGNMENT = "enableThroughputBasedPartitionAssignment";
  public static final String CFG_ENABLE_PARTITION_NUM_BASED_TASK_COUNT_ESTIMATION = "enablePartitionNumBasedTaskCountEstimation";
  public static final String CFG_DEFAULT_PARTITION_BYTES_IN_KB_RATE = "defaultPartitionBytesInKBRate";
  public static final String CFG_DEFAULT_PARTITION_MSGS_IN_RATE = "defaultPartitionMsgsInRate";

  private static final int DEFAULT_THROUGHPUT_INFO_FETCH_TIMEOUT_MS = (int) Duration.ofSeconds(10).toMillis();
  private static final int DEFAULT_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS = (int) Duration.ofSeconds(1).toMillis();
  private static final int DEFAULT_TASK_CAPACITY_MBPS = 4;
  private static final int DEFAULT_TASK_CAPACITY_UTILIZATION_PCT = 90;
  private static final boolean DEFAULT_ENABLE_THROUGHPUT_BASED_PARTITION_ASSIGNMENT = false;
  private static final boolean DEFAULT_ENABLE_PARTITION_NUM_BASED_TASK_COUNT_ESTIMATION = false;
  private static final int DEFAULT_PARTITION_BYTES_IN_KB_RATE = 5;
  private static final int DEFAULT_PARTITION_MSGS_IN_RATE = 5;


  private final int _taskCapacityMBps;
  private final int _taskCapacityUtilizationPct;
  private final int _throughputInfoFetchTimeoutMs;
  private final int _throughputInfoFetchRetryPeriodMs;
  private final boolean _enableThroughputBasedPartitionAssignment;
  private final boolean _enablePartitionNumBasedTaskCountEstimation;
  private final int _defaultPartitionBytesInKBRate;
  private final int _defaultPartitionMsgsInRate;

  /**
   * Creates an instance of {@link LoadBasedPartitionAssignmentStrategyConfig}
   * @param config Config properties
   */
  public LoadBasedPartitionAssignmentStrategyConfig(Properties config) {
    super(config);
    VerifiableProperties props = new VerifiableProperties(config);
    _taskCapacityMBps = props.getInt(CFG_TASK_CAPACITY_MBPS, DEFAULT_TASK_CAPACITY_MBPS);
    _taskCapacityUtilizationPct = props.getIntInRange(CFG_TASK_CAPACITY_UTILIZATION_PCT, DEFAULT_TASK_CAPACITY_UTILIZATION_PCT, 0, 100);
    _throughputInfoFetchTimeoutMs = props.getInt(CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS, DEFAULT_THROUGHPUT_INFO_FETCH_TIMEOUT_MS);
    _throughputInfoFetchRetryPeriodMs = props.getInt(CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS, DEFAULT_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS);
    _enableThroughputBasedPartitionAssignment = props.getBoolean(CFG_ENABLE_THROUGHPUT_BASED_PARTITION_ASSIGNMENT,
        DEFAULT_ENABLE_THROUGHPUT_BASED_PARTITION_ASSIGNMENT);
    _enablePartitionNumBasedTaskCountEstimation = props.getBoolean(CFG_ENABLE_PARTITION_NUM_BASED_TASK_COUNT_ESTIMATION,
        DEFAULT_ENABLE_PARTITION_NUM_BASED_TASK_COUNT_ESTIMATION);
    _defaultPartitionBytesInKBRate = props.getInt(CFG_DEFAULT_PARTITION_BYTES_IN_KB_RATE, DEFAULT_PARTITION_BYTES_IN_KB_RATE);
    _defaultPartitionMsgsInRate = props.getInt(CFG_DEFAULT_PARTITION_MSGS_IN_RATE, DEFAULT_PARTITION_MSGS_IN_RATE);
  }

  /**
   * Gets task capacity measured in MB/sec
   * @return Task capacity in MB/sec
   */
  public int getTaskCapacityMBps() {
    return _taskCapacityMBps;
  }

  /**
   * Gets task capacity utilization percentage
   * @return Task capacity utilization percentage
   */
  public int getTaskCapacityUtilizationPct() {
    return _taskCapacityUtilizationPct;
  }

  /**
   * Gets throughput info fetch timeout in milliseconds
   * @return Throughput info fetch timeout in milliseconds
   */
  public int getThroughputInfoFetchTimeoutMs() {
    return _throughputInfoFetchTimeoutMs;
  }

  /**
   * Gets the throughput info fetch retry period in milliseconds
   * @return Throughput info fetch retry period in milliseconds
   */
  public int getThroughputInfoFetchRetryPeriodMs() {
    return _throughputInfoFetchRetryPeriodMs;
  }

  /**
   * Check if throughput based partition assignment is enabled or not
   * @return True if throughput based partition assignment is enabled else false
   */
  public boolean isEnableThroughputBasedPartitionAssignment() {
    return _enableThroughputBasedPartitionAssignment;
  }

  /**
   * Check if partition number based task count estimation is enabled or not
   * @return True if partition number based task count estimation is enabled else false
   */
  public boolean isEnablePartitionNumBasedTaskCountEstimation() {
    return _enablePartitionNumBasedTaskCountEstimation;
  }

  public int getDefaultPartitionBytesInKBRate() {
    return _defaultPartitionBytesInKBRate;
  }

  public int getDefaultPartitionMsgsInRate() {
    return _defaultPartitionMsgsInRate;
  }
}
