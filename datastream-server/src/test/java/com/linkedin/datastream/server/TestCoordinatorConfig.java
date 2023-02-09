/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for {@link com.linkedin.datastream.server.CoordinatorConfig}
 */
@Test
public class TestCoordinatorConfig {
  private static final String CLUSTER = "brooklin";
  private static final String ZK_ADDRESS = "localhost:9999";

  private CoordinatorConfig createCoordinatorConfig(Properties props) {
    props.put(CoordinatorConfig.CONFIG_CLUSTER, CLUSTER);
    props.put(CoordinatorConfig.CONFIG_ZK_ADDRESS, ZK_ADDRESS);
    CoordinatorConfig config = new CoordinatorConfig(props);
    return config;
  }

  @Test
  public void testCoordinatorMaxAssignmentRetryCountFromConfig() throws Exception {
    Properties props = new Properties();
    props.put(CoordinatorConfig.CONFIG_MAX_ASSIGNMENT_RETRY_COUNT, "10");
    CoordinatorConfig config = createCoordinatorConfig(props);
    Assert.assertEquals(10, config.getMaxAssignmentRetryCount());
  }

  @Test
  public void testCoordinatorMaxAssignmentRetryCountDefault() throws Exception {
    Properties props = new Properties();
    CoordinatorConfig config = createCoordinatorConfig(props);
    Assert.assertEquals(CoordinatorConfig.DEFAULT_MAX_ASSIGNMENT_RETRY_COUNT, config.getMaxAssignmentRetryCount());
  }

  @Test
  public void testStopPropagationTimeoutConfig() {
    Properties props = new Properties();
    CoordinatorConfig config = createCoordinatorConfig(props);
    Assert.assertEquals(CoordinatorConfig.DEFAULT_STOP_PROPAGATION_TIMEOUT_MS, config.getStopPropagationTimeout());

    String stopPropagationTimeoutValue = "1000";
    props.put(CoordinatorConfig.CONFIG_STOP_PROPAGATION_TIMEOUT_MS, stopPropagationTimeoutValue);
    CoordinatorConfig config2 = createCoordinatorConfig(props);
    Assert.assertEquals(config2.getStopPropagationTimeout(), 1000);
  }

  @Test
  public void testTaskStopTimeoutAndRetryConfigDefault() {
    Properties props = new Properties();
    CoordinatorConfig config = createCoordinatorConfig(props);
    Assert.assertEquals(CoordinatorConfig.DEFAULT_TASK_STOP_CHECK_RETRY_PERIOD_MS, config.getTaskStopCheckRetryPeriodMs());
    Assert.assertEquals(CoordinatorConfig.DEFAULT_TASK_STOP_CHECK_TIMEOUT_MS, config.getTaskStopCheckTimeoutMs());
  }

  @Test
  public void testForceStopStreamsOnFailureConfig() {
    Properties props = new Properties();
    CoordinatorConfig config = createCoordinatorConfig(props);
    Assert.assertFalse(config.getForceStopStreamsOnFailure());
    props.put(CoordinatorConfig.CONFIG_FORCE_STOP_STREAMS_ON_FAILURE, "true");
    config = createCoordinatorConfig(props);
    Assert.assertTrue(config.getForceStopStreamsOnFailure());
  }
}
