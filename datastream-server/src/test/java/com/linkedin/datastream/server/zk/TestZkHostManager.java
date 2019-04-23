/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.zk;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ZkHostManager}
 */
public class TestZkHostManager {

  @Test
  public void testHappyPath() {
    Properties properties = new Properties();
    properties.put("brooklin.server.coordinator.zkHost.enableBlacklist", "true");
    ZkHostManager zkHostManager = new ZkHostManager(new ZkHostConfig("brooklin.server.coordinator.", properties));
    List<String> instances = Arrays.asList("host1-001", "host2-002", "host3-003");
    List<String> stableInstances = zkHostManager.joinHosts(instances);
    Assert.assertEquals(stableInstances, instances);
  }

  @Test
  public void testDisableBlackList() {
    Properties properties = new Properties();
    properties.put("brooklin.server.coordinator.zkHost.enableBlacklist", "false");
    properties.put("brooklin.server.coordinator.zkHost.maxJoinNum", "2");

    ZkHostManager zkHostManager = new ZkHostManager(new ZkHostConfig("brooklin.server.coordinator.", properties));
    List<String> instances1 = Arrays.asList("us-host1-001", "us-host2-002", "us-host3-003");
    List<String> instances2 = Arrays.asList("us-host1-001", "us-host2-005", "us-host3-003");
    List<String> instances3 = Arrays.asList("us-host1-002", "us-host2-006", "us-host3-003");

    zkHostManager.joinHosts(instances1);
    zkHostManager.joinHosts(instances2);

    List<String> stableInstances = zkHostManager.joinHosts(instances3);
    List<String> expectedList = Arrays.asList("us-host1-002", "us-host2-006", "us-host3-003");
    Assert.assertEquals(stableInstances, expectedList);
  }

  @Test
  public void testFilterUnstableHosts() throws Exception {
    Properties properties = new Properties();
    properties.put("brooklin.server.coordinator.zkHost.enableBlacklist", "true");
    properties.put("brooklin.server.coordinator.zkHost.maxJoinNum", "2");
    properties.put("brooklin.server.coordinator.zkHost.blacklistedDurationMs", "900");


    ZkHostManager zkHostManager = new ZkHostManager(new ZkHostConfig("brooklin.server.coordinator.", properties));
    List<String> instances1 = Arrays.asList("us-host1-001", "us-host2-002", "us-host3-003");
    List<String> instances2 = Arrays.asList("us-host1-001", "us-host2-005", "us-host3-003");
    List<String> instances3 = Arrays.asList("us-host1-002", "us-host2-006", "us-host3-003");

    zkHostManager.joinHosts(instances1);
    zkHostManager.joinHosts(instances2);

    List<String> stableInstances = zkHostManager.joinHosts(instances3);
    List<String> expectedList = Arrays.asList("us-host1-002", "us-host3-003");
    Assert.assertEquals(stableInstances, expectedList);
    Thread.sleep(1000);
    List<String> instances4 = Arrays.asList("us-host1-002", "us-host2-007", "us-host3-003");

    List<String> stableInstances2 = zkHostManager.joinHosts(instances4);
    Assert.assertEquals(stableInstances2, Arrays.asList("us-host1-002", "us-host2-007", "us-host3-003"));
  }

  @Test
  public void testStabilizedWindow() throws Exception {
    Properties properties = new Properties();
    properties.put("brooklin.server.coordinator.zkHost.enableBlacklist", "true");
    properties.put("brooklin.server.coordinator.zkHost.maxJoinNum", "2");
    properties.put("brooklin.server.coordinator.zkHost.stabilizedWindowMs", "900");


    ZkHostManager zkHostManager = new ZkHostManager(new ZkHostConfig("brooklin.server.coordinator.", properties));
    List<String> instances1 = Arrays.asList("us-host1-001", "us-host2-002", "us-host3-003");
    List<String> instances2 = Arrays.asList("us-host1-001", "us-host2-005", "us-host3-003");
    List<String> instances3 = Arrays.asList("us-host1-002", "us-host2-006", "us-host3-003");

    zkHostManager.joinHosts(instances1);
    zkHostManager.joinHosts(instances2);
    Thread.sleep(1000);
    List<String> stableInstances = zkHostManager.joinHosts(instances3);
    List<String> expectedList = Arrays.asList("us-host1-002", "us-host2-006", "us-host3-003");

    Assert.assertEquals(stableInstances, expectedList);
  }
}
