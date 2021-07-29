/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
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

  @BeforeMethod
  public void setup() {
  }

  @AfterMethod
  public void teardown() {
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
}
