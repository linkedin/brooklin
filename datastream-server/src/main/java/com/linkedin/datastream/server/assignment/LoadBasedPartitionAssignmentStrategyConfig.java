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

  public static final int DEFAULT_PARTITION_BYTES_IN_KB_RATE = 5;
  public static final int DEFAULT_PARTITION_MESSAGES_IN_RATE = 5;

  private static final int DEFAULT_THROUGHPUT_INFO_FETCH_TIMEOUT_MS = (int) Duration.ofSeconds(10).toMillis();
  private static final int DEFAULT_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS = (int) Duration.ofSeconds(1).toMillis();
  private static final int DEFAULT_TASK_CAPACITY_MBPS = 4;
  private static final int DEFAULT_TASK_CAPACITY_UTILIZATION_PCT = 90;

  private final int _taskCapacityMBps;
  private final int _taskCapacityUtilizationPct;
  private final int _throughputInfoFetchTimeoutMs;
  private final int _throughputInfoFetchRetryPeriodMs;
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
}
