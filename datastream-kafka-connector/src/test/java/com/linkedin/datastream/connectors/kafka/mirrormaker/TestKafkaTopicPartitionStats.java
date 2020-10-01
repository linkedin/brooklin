/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableSet;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.kafka.KafkaTopicPartitionStatsResponse;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;


/**
 * Tests for TopicPartitionStats diag command
 */
@Test
public class TestKafkaTopicPartitionStats extends BaseKafkaZkTest {

  private static final int PARTITION_COUNT = 2;
  private static final String PARTITIONS = "/partitions";

  @Test
  public void testProcessTopicPartitionStats() {
    List<String> topics = Arrays.asList("topic1", "topic2");
    topics.forEach(topic -> createTopic(_zkUtils, topic, PARTITION_COUNT));

    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("topicStream", _broker, "topic\\d+");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("MirrorMakerConnector",
        KafkaMirrorMakerConnectorTestUtils.getDefaultConfig(Optional.empty()), "testCluster");
    connector.start(null);

    // Notify connector of paused partition update
    connector.onAssignmentChange(Collections.singletonList(task));

    Map<String, Set<Integer>> expected = new HashMap<>();
    topics.forEach(topic -> {
      for (int i = 0; i < PARTITION_COUNT; ++i) {
        Set<Integer> partitions = expected.computeIfAbsent(topic, k -> new HashSet<>());
        partitions.add(i);
      }
    });

    // Wait until the partitions are assigned
    if (!PollUtils.poll(() -> testProcessTopicPartitionsStatsInternal(connector, expected),
        KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS, KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS)) {
      Assert.fail("Topic partitions still not assigned");
    }

    // Delete the topic to revoke partitions from topic2
    deleteTopic(_zkUtils, topics.get(0));
    expected.remove(topics.get(0));

    // Wait until the partitions from the deleted topic are revoked
    if (!PollUtils.poll(() -> testProcessTopicPartitionsStatsInternal(connector, expected),
        KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS, KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS)) {
      Assert.fail("Topic partitions still not revoked");
    }
    connector.stop();
  }

  private boolean testProcessTopicPartitionsStatsInternal(KafkaMirrorMakerConnector connector,
      Map<String, Set<Integer>> expected) {
    String jsonStr = connector.process(PARTITIONS);
    List<KafkaTopicPartitionStatsResponse> responseList =
        JsonUtils.fromJson(jsonStr, new TypeReference<List<KafkaTopicPartitionStatsResponse>>() {
        });

    Map<String, Set<Integer>> actual = new HashMap<>();
    responseList.forEach(response -> {
      Map<String, Set<Integer>> partitions = response.getTopicPartitions();
      actual.putAll(response.getTopicPartitions());
    });

    return actual.equals(expected);
  }

  @Test
  public void testReduceTopicPartitionStats() {
    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("KafkaConnector",
        KafkaMirrorMakerConnectorTestUtils.getDefaultConfig(Optional.empty()), "testCluster");

    String topic3 = "topic3";
    String topic4 = "topic4";

    String instance1 = "instance1";
    String instance2 = "instance2";

    String consumerGroup1 = "cg1";
    String consumerGroup2 = "cg2";

    // build instance 1 results
    List<KafkaTopicPartitionStatsResponse> responseList1 = new ArrayList<>();
    responseList1.add(
        new KafkaTopicPartitionStatsResponse(consumerGroup1, instance1, Collections.singletonMap(topic3,
                Collections.singleton(0)), Collections.singleton("test1Stream")));
    responseList1.add(
        new KafkaTopicPartitionStatsResponse(consumerGroup1, instance1, Collections.singletonMap(topic3,
                Collections.singleton(2)), Collections.singleton("test1Stream")));
    responseList1.add(
        new KafkaTopicPartitionStatsResponse(consumerGroup2, instance1, Collections.singletonMap(topic4,
                Collections.singleton(1)), Collections.singleton("test2Stream")));

    // build instance 2 results
    List<KafkaTopicPartitionStatsResponse> responseList2 = new ArrayList<>();
    responseList2.add(
        new KafkaTopicPartitionStatsResponse(consumerGroup2, instance2, Collections.singletonMap(topic4,
                Collections.singleton(0)), Collections.singleton("test2Stream")));
    responseList2.add(
        new KafkaTopicPartitionStatsResponse(consumerGroup2, instance2, Collections.singletonMap(topic4,
                Collections.singleton(2)), Collections.singleton("test2Stream")));
    responseList2.add(
        new KafkaTopicPartitionStatsResponse(consumerGroup1, instance2, Collections.singletonMap(topic3,
                Collections.singleton(1)), Collections.singleton("test1Stream")));

    Map<String, String> responseMap = new HashMap<>();
    responseMap.put(instance1, JsonUtils.toJson(responseList1));
    responseMap.put(instance2, JsonUtils.toJson(responseList2));

    String json = connector.reduce(PARTITIONS, responseMap);
    List<KafkaTopicPartitionStatsResponse> responseList =
        JsonUtils.fromJson(json, new TypeReference<List<KafkaTopicPartitionStatsResponse>>() {
        });
    Assert.assertEquals(responseList.size(), 6, "Should have received flattened list with 6 responses");

    responseList.forEach(response -> {
      Assert.assertNotNull(response.getTopicPartitions());
      Assert.assertNotNull(response.getSourceInstance());
      if (response.getTopicPartitions().containsKey(topic3)) {
        response.getTopicPartitions().get(topic3).removeAll(ImmutableSet.of(0, 1, 2));
        Assert.assertTrue(response.getTopicPartitions().get(topic3).isEmpty());
      } else {
        response.getTopicPartitions().get(topic4).removeAll(ImmutableSet.of(0, 1, 2));
        Assert.assertTrue(response.getTopicPartitions().get(topic4).isEmpty());
      }
    });
  }
}
