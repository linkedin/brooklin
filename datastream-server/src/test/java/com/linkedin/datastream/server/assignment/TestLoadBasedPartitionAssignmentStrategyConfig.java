/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.assignment;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link LoadBasedPartitionAssignmentStrategyConfig}
 */
public class TestLoadBasedPartitionAssignmentStrategyConfig {
  private static final String CFG_TASK_CAPACITY_MBPS_VALUE = "5";
  private static final String CFG_TASK_CAPACITY_UTILIZATION_PCT_VALUE = "82";
  private static final String CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS_VALUE = "1100";
  private static final String CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS_VALUE = "1200";

  @Test
  public void configValuesCorrectlyAssignedTest() {
    Properties props = new Properties();

    props.setProperty(LoadBasedPartitionAssignmentStrategyConfig.CFG_TASK_CAPACITY_MBPS, CFG_TASK_CAPACITY_MBPS_VALUE);
    props.setProperty(LoadBasedPartitionAssignmentStrategyConfig.CFG_TASK_CAPACITY_UTILIZATION_PCT,
        CFG_TASK_CAPACITY_UTILIZATION_PCT_VALUE);
    props.setProperty(LoadBasedPartitionAssignmentStrategyConfig.CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS,
        CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS_VALUE);
    props.setProperty(LoadBasedPartitionAssignmentStrategyConfig.CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS,
        CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS_VALUE);
    props.setProperty(LoadBasedPartitionAssignmentStrategyConfig.CFG_ENABLE_PARTITION_NUM_BASED_TASK_COUNT_ESTIMATION,
        String.valueOf(false));
    props.setProperty(LoadBasedPartitionAssignmentStrategyConfig.CFG_ENABLE_THROUGHPUT_BASED_PARTITION_ASSIGNMENT, String.valueOf(true));

    LoadBasedPartitionAssignmentStrategyConfig config = new LoadBasedPartitionAssignmentStrategyConfig(props);

    Assert.assertEquals(config.getTaskCapacityMBps(), Integer.parseInt(CFG_TASK_CAPACITY_MBPS_VALUE));
    Assert.assertEquals(config.getTaskCapacityUtilizationPct(), Integer.parseInt(CFG_TASK_CAPACITY_UTILIZATION_PCT_VALUE));
    Assert.assertEquals(config.getThroughputInfoFetchTimeoutMs(), Integer.parseInt(CFG_THROUGHPUT_INFO_FETCH_TIMEOUT_MS_VALUE));
    Assert.assertEquals(config.getThroughputInfoFetchRetryPeriodMs(), Integer.parseInt(CFG_THROUGHPUT_INFO_FETCH_RETRY_PERIOD_MS_VALUE));
    Assert.assertTrue(config.isEnableThroughputBasedPartitionAssignment());
    Assert.assertFalse(config.isEnablePartitionNumBasedTaskCountEstimation());
  }
}
