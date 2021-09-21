/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.common.DatastreamRuntimeException;
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
  protected String _broker;
  protected AdminClient _adminClient;

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
    Properties adminClientProps = new Properties();
    adminClientProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, _broker);
    _adminClient = AdminClient.create(adminClientProps);

    _zkClient = new ZkClient(_kafkaCluster.getZkConnection());
  }

  @AfterMethod(alwaysRun = true)
  public void afterMethodTeardown() {
    if (_zkClient != null) {
      _zkClient.close();
    }
    if (_kafkaCluster != null && _kafkaCluster.isStarted()) {
      _kafkaCluster.shutdown();
    }
    _broker = null;
  }

  protected static void createTopic(AdminClient adminClient, String topic, int partitionCount) {
    NewTopic newTopic = new NewTopic(topic, partitionCount, (short) 1);
    try {
      adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      if (e.getCause() instanceof TopicExistsException) {
        return;
      } else {
        throw new DatastreamRuntimeException(e);
      }
    }
  }

  protected static void createTopic(AdminClient adminClient, String topic) {
    createTopic(adminClient, topic, 1);
  }

  protected static void deleteTopic(AdminClient adminClient, String topic) {
    try {
      adminClient.deleteTopics(Collections.singletonList(topic)).all().get();
    } catch (InterruptedException | ExecutionException e) {
      if (e instanceof UnknownTopicOrPartitionException || e.getCause() instanceof UnknownTopicOrPartitionException) {
        Assert.fail(String.format("Exception caught while deleting topic %s", topic));
      }
    }
  }

  protected static boolean topicExists(AdminClient adminClient, String topic) {
    try {
      Map<String, TopicListing> topicListingMap = adminClient.listTopics().namesToListings().get();
      if (topicListingMap.containsKey(topic)) {
        return true;
      }
    } catch (InterruptedException | ExecutionException e) {
      return false;
    }
    return false;
  }
}