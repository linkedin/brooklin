/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil;

import java.util.Properties;

import org.I0Itec.zkclient.ZkConnection;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.metrics.DynamicMetricsManager;


/**
 * Base test class for test cases that rely on {@link DatastreamEmbeddedZookeeperKafkaCluster} so that all tests
 * in a given class could share the same ZooKeeper-Kafka cluster and zk client/utils.
 */
@Test
public abstract class BaseKafkaZkTest {

  protected DatastreamEmbeddedZookeeperKafkaCluster _kafkaCluster;
  protected ZkClient _zkClient;
  protected ZkUtils _zkUtils;
  protected String _broker;

  @BeforeMethod(alwaysRun = true)
  public void beforeMethodSetup() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    Properties kafkaConfig = new Properties();
    // we will disable auto topic creation for tests
    kafkaConfig.setProperty("auto.create.topics.enable", Boolean.FALSE.toString());
    kafkaConfig.setProperty("delete.topic.enable", Boolean.TRUE.toString());
    kafkaConfig.setProperty("offsets.topic.replication.factor", "1");
    _kafkaCluster = new DatastreamEmbeddedZookeeperKafkaCluster(kafkaConfig);
    _kafkaCluster.startup();
    _broker = _kafkaCluster.getBrokers().split("\\s*,\\s*")[0];
    _zkClient = new ZkClient(_kafkaCluster.getZkConnection());
    _zkUtils = new ZkUtils(_zkClient, new ZkConnection(_kafkaCluster.getZkConnection()), false);
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethodTeardown() {
    if (_zkUtils != null) {
      _zkUtils.close();
    }
    if (_zkClient != null) {
      _zkClient.close();
    }
    if (_kafkaCluster != null && _kafkaCluster.isStarted()) {
      _kafkaCluster.shutdown();
    }
    _broker = null;
  }

  protected static void createTopic(ZkUtils zkUtils, String topic, int partitionCount) {
    if (!AdminUtils.topicExists(zkUtils, topic)) {
      Properties properties = new Properties();
      properties.put("message.timestamp.type", "LogAppendTime");
      AdminUtils.createTopic(zkUtils, topic, partitionCount, 1, properties, null);
    }
  }

  protected static void createTopic(ZkUtils zkUtils, String topic) {
    createTopic(zkUtils, topic, 1);
  }

  protected static void deleteTopic(ZkUtils zkUtils, String topic) {
    if (AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.deleteTopic(zkUtils, topic);

      if (!PollUtils.poll(() -> !AdminUtils.topicExists(zkUtils, topic), 100, 25000)) {
        Assert.fail("topic was not properly deleted " + topic);
      }
    }
  }
}