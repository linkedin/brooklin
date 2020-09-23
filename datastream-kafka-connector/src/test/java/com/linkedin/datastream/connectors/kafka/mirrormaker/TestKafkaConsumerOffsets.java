/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.kafka.KafkaConnectorDiagUtils;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerOffsetsResponse;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;


/**
 * Tests for kafka consumer offsets diagnostic endpoint
 */
@Test
public class TestKafkaConsumerOffsets extends BaseKafkaZkTest {

  private static final int TOPIC_COUNT = 2;
  private static final int PARTITION_COUNT = 2;
  private static final int PARTITION_MESSAGE_COUNT = 10;
  private static final String CONSUMER_OFFSETS = "/consumer_offsets";
  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaConsumerOffsets.class);

  @Test
  public void testConsumerOffsetsDiagEndpoint() {
    // create topics
    List<String> topics = new ArrayList<>();
    IntStream.range(0, TOPIC_COUNT).forEach(i -> topics.add("topic" + i));
    topics.forEach(topic -> createTopic(_zkUtils, topic, PARTITION_COUNT));

    // setup datastream and connector
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("topicStream", _broker, "topic\\d+");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("MirrorMakerConnector",
        KafkaMirrorMakerConnectorTestUtils.getDefaultConfig(Optional.empty()), "testCluster");
    connector.start(null);

    // produce messages to each topic partition
    topics.forEach(topic -> IntStream.range(0, PARTITION_COUNT).forEach(partition ->
        KafkaMirrorMakerConnectorTestUtils.produceEventsToPartition(topic, partition, PARTITION_MESSAGE_COUNT, _kafkaCluster)));

    connector.onAssignmentChange(Collections.singletonList(task));

    // wait until the consumer offsets are updated for each topic partition
    if (!PollUtils.poll(() -> testConsumerOffsetsAreUpdated(connector),
        KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS, KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS)) {
      Assert.fail("Consumer offsets were not updated correctly");
    }

    // shutdown
    connector.stop();
  }

  @Test
  public void testConsumerOffsetsReducer() {
    String topic1 = "topic1";
    String topic2 = "topic2";

    String consumerGroup1 = "cg1";
    String consumerGroup2 = "cg2";
    String consumerGroup3 = "cg3";

    String instance1 = "i1";
    String instance2 = "i2";

    // constructing instance1 consumer offsets
    List<KafkaConsumerOffsetsResponse> responseList1 = new ArrayList<>();

    // instance 1 consumer group 1
    Map<String, Map<Integer, Long>> topicPartitionOffsets1 = new HashMap<>();

    Map<Integer, Long> partitionOffsets1 = new HashMap<>();
    partitionOffsets1.put(0, 10L);
    partitionOffsets1.put(1, 10L);
    topicPartitionOffsets1.put(topic1, partitionOffsets1);

    Map<Integer, Long> partitionOffsets2 = new HashMap<>();
    partitionOffsets2.put(0, 10L);
    partitionOffsets2.put(1, 10L);
    topicPartitionOffsets1.put(topic2, partitionOffsets2);

    responseList1.add(new KafkaConsumerOffsetsResponse(topicPartitionOffsets1, consumerGroup1));

    // instance 1 consumer group 2
    Map<String, Map<Integer, Long>> topicPartitionOffsets2 = new HashMap<>();

    Map<Integer, Long> partitionOffsets3 = new HashMap<>();
    partitionOffsets3.put(0, 20L);
    partitionOffsets3.put(1, 20L);
    topicPartitionOffsets2.put(topic1, partitionOffsets3);

    Map<Integer, Long> partitionOffsets4 = new HashMap<>();
    partitionOffsets4.put(0, 20L);
    partitionOffsets4.put(1, 20L);
    topicPartitionOffsets2.put(topic2, partitionOffsets4);

    responseList1.add(new KafkaConsumerOffsetsResponse(topicPartitionOffsets2, consumerGroup2));

    // constructing instance2 consumer offsets
    List<KafkaConsumerOffsetsResponse> responseList2 = new ArrayList<>();

    // instance 2 consumer group 1
    Map<String, Map<Integer, Long>> topicPartitionOffsets3 = new HashMap<>();

    Map<Integer, Long> partitionOffsets5 = new HashMap<>();
    partitionOffsets5.put(2, 10L);
    partitionOffsets5.put(3, 10L);
    topicPartitionOffsets3.put(topic1, partitionOffsets5);

    responseList2.add(new KafkaConsumerOffsetsResponse(topicPartitionOffsets3, consumerGroup1));

    // instance 2 consumer group 3
    Map<String, Map<Integer, Long>> topicPartitionOffsets4 = new HashMap<>();

    Map<Integer, Long> partitionOffsets6 = new HashMap<>();
    partitionOffsets6.put(0, 30L);
    topicPartitionOffsets4.put(topic2, partitionOffsets6);
    responseList2.add(new KafkaConsumerOffsetsResponse(topicPartitionOffsets4, consumerGroup3));

    // reducing responses and asserting correctness
    Map<String, String> responseMap = new HashMap<>();
    responseMap.put(instance1, JsonUtils.toJson(responseList1));
    responseMap.put(instance2, JsonUtils.toJson(responseList2));

    String reducedMapJson = KafkaConnectorDiagUtils.reduceConsumerOffsetsResponses(responseMap, LOG);
    List<KafkaConsumerOffsetsResponse> responseList =
        JsonUtils.fromJson(reducedMapJson, new TypeReference<List<KafkaConsumerOffsetsResponse>>() { });

    Assert.assertEquals(responseList.size(), 3); // 3 consumer groups

    KafkaConsumerOffsetsResponse cg1Response = responseList.stream().
        filter(r -> r.getConsumerGroupId().equals(consumerGroup1)).findAny().orElse(null);
    Assert.assertNotNull(cg1Response);
    Assert.assertEquals(cg1Response.getConsumerOffsets().keySet().size(), 2); // cg1 consumes both topics
    Assert.assertEquals(cg1Response.getConsumerOffsets().get(topic1).keySet().size(), 4); // cg1 consumes 4 partitions for topic 1
    Assert.assertEquals(cg1Response.getConsumerOffsets().get(topic2).keySet().size(), 2); // cg1 consumes 2 partitions for topic 2

    KafkaConsumerOffsetsResponse cg2Response = responseList.stream().
        filter(r -> r.getConsumerGroupId().equals(consumerGroup2)).findAny().orElse(null);
    Assert.assertNotNull(cg2Response);
    Assert.assertEquals(cg2Response.getConsumerOffsets().keySet().size(), 2); // cg2 consumers both topics
    Assert.assertEquals(cg2Response.getConsumerOffsets().get(topic1).keySet().size(), 2); // cg2 consumes 2 partitions for topic 1
    Assert.assertEquals(cg2Response.getConsumerOffsets().get(topic2).keySet().size(), 2); // cg2 consumes 2 partitions for topic 2

    KafkaConsumerOffsetsResponse cg3Response = responseList.stream().
        filter(r -> r.getConsumerGroupId().equals(consumerGroup3)).findAny().orElse(null);
    Assert.assertNotNull(cg3Response);
    Assert.assertEquals(cg3Response.getConsumerOffsets().keySet().size(), 1); // cg3 consumes only topic 2
    Assert.assertEquals(cg3Response.getConsumerOffsets().get(topic2).size(), 1); // cg3 consumes 1 partition for topic 2
  }

  private boolean testConsumerOffsetsAreUpdated(KafkaMirrorMakerConnector connector) {
    String jsonStr = connector.process(CONSUMER_OFFSETS);
    List<KafkaConsumerOffsetsResponse> responseList =
        JsonUtils.fromJson(jsonStr, new TypeReference<List<KafkaConsumerOffsetsResponse>>() {
        });

    if (responseList == null || responseList.size() != 1) {
      return false;
    }
    KafkaConsumerOffsetsResponse offsetResponse = responseList.get(0);

    // check that all topic partitions were polled and offsets were updated
    boolean allTopicsWerePolled = offsetResponse.getConsumerOffsets().size() == TOPIC_COUNT;
    boolean allPartitionsWerePolled = offsetResponse.getConsumerOffsets().values().stream().
        allMatch(m -> m.keySet().size() == PARTITION_COUNT);

    if (!allTopicsWerePolled || !allPartitionsWerePolled) {
      return false;
    }

    for (String topic : offsetResponse.getConsumerOffsets().keySet()) {
      Map<Integer, Long> partitionOffsets = offsetResponse.getConsumerOffsets().get(topic);

      for (Integer partition : partitionOffsets.keySet()) {
        // check consumer offsets. Note that offsets are zero based
        if (partitionOffsets.get(partition) != PARTITION_MESSAGE_COUNT - 1) {
          return false;
        }
      }
    }

    return true;
  }
}
