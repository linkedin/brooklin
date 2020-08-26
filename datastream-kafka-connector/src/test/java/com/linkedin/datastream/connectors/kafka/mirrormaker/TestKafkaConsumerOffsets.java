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
import com.linkedin.datastream.connectors.kafka.KafkaConsumerOffsetsResponse;
import com.linkedin.datastream.connectors.kafka.KafkaConnectorDiagUtils;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;


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

    // wait until the consumer offsets are updated
    if (!PollUtils.poll(() -> testConsumerOffsetsAreUpdated(connector),
        KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS, KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS)) {
      Assert.fail("Topic partitions still not assigned");
    }

    // query consumer offsets and assert that they're correct
    String jsonStr = connector.process(CONSUMER_OFFSETS);
    List<KafkaConsumerOffsetsResponse> responseList =
        JsonUtils.fromJson(jsonStr, new TypeReference<List<KafkaConsumerOffsetsResponse>>() {
        });

    Assert.assertEquals(responseList.size(), 1);
    KafkaConsumerOffsetsResponse offsetResponse = responseList.get(0);

    Assert.assertEquals(offsetResponse.getConsumerOffsets().size(), TOPIC_COUNT);
    Map<String, Map<Integer, Long>> consumerOffsets = offsetResponse.getConsumerOffsets();

    consumerOffsets.values().forEach(partitionOffsets -> Assert.assertEquals(partitionOffsets.size(), PARTITION_COUNT));
    consumerOffsets.values().forEach(partitionOffsets -> partitionOffsets.values()
        .forEach(offset -> Assert.assertEquals((long)offset, PARTITION_MESSAGE_COUNT - 1))); // 0-based offsets

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
    List<KafkaConsumerOffsetsResponse> i1_responseList = new ArrayList<>();

    // instance 1 consumer group 1
    Map<String, Map<Integer, Long>> i1_cg1_topicPartitionOffsets = new HashMap<>();

    Map<Integer, Long> i1_cg1_t1_partitionOffsets = new HashMap<>();
    i1_cg1_t1_partitionOffsets.put(0, 10L);
    i1_cg1_t1_partitionOffsets.put(1, 10L);
    i1_cg1_topicPartitionOffsets.put(topic1, i1_cg1_t1_partitionOffsets);

    Map<Integer, Long> i1_cg1_t2_partitionOffsets = new HashMap<>();
    i1_cg1_t2_partitionOffsets.put(0, 10L);
    i1_cg1_t2_partitionOffsets.put(1, 10L);
    i1_cg1_topicPartitionOffsets.put(topic2, i1_cg1_t2_partitionOffsets);

    i1_responseList.add(new KafkaConsumerOffsetsResponse(i1_cg1_topicPartitionOffsets, consumerGroup1));

    // instance 1 consumer group 2
    Map<String, Map<Integer, Long>> i1_cg2_topicPartitionOffsets = new HashMap<>();

    Map<Integer, Long> i1_cg2_t1_partitionOffsets = new HashMap<>();
    i1_cg2_t1_partitionOffsets.put(0, 20L);
    i1_cg2_t1_partitionOffsets.put(1, 20L);
    i1_cg2_topicPartitionOffsets.put(topic1, i1_cg2_t1_partitionOffsets);

    Map<Integer, Long> i1_cg2_t2_partitionOffsets = new HashMap<>();
    i1_cg2_t2_partitionOffsets.put(0, 20L);
    i1_cg2_t2_partitionOffsets.put(1, 20L);
    i1_cg2_topicPartitionOffsets.put(topic2, i1_cg2_t2_partitionOffsets);

    i1_responseList.add(new KafkaConsumerOffsetsResponse(i1_cg2_topicPartitionOffsets, consumerGroup2));

    // constructing instance2 consumer offsets
    List<KafkaConsumerOffsetsResponse> i2_responseList = new ArrayList<>();

    // instance 2 consumer group 1
    Map<String, Map<Integer, Long>> i2_cg1_topicPartitionOffsets = new HashMap<>();

    Map<Integer, Long> i2_cg1_t1_partitionOffsets = new HashMap<>();
    i2_cg1_t1_partitionOffsets.put(2, 10L);
    i2_cg1_t1_partitionOffsets.put(3, 10L);
    i2_cg1_topicPartitionOffsets.put(topic1, i2_cg1_t1_partitionOffsets);

    i2_responseList.add(new KafkaConsumerOffsetsResponse(i2_cg1_topicPartitionOffsets, consumerGroup1));

    // instance 2 consumer group 3
    Map<String, Map<Integer, Long>> i2_cg3_topicPartitionOffsets = new HashMap<>();

    Map<Integer, Long> i2_cg3_t2_partitionOffsets = new HashMap<>();
    i2_cg3_t2_partitionOffsets.put(0, 30L);
    i2_cg3_topicPartitionOffsets.put(topic2, i2_cg3_t2_partitionOffsets);
    i2_responseList.add(new KafkaConsumerOffsetsResponse(i2_cg3_topicPartitionOffsets, consumerGroup3));

    // reducing responses and asserting correctness
    Map<String, String> responseMap = new HashMap<>();
    responseMap.put(instance1, JsonUtils.toJson(i1_responseList));
    responseMap.put(instance2, JsonUtils.toJson(i2_responseList));

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

    Assert.assertEquals(responseList.size(), 1);
    KafkaConsumerOffsetsResponse offsetResponse = responseList.get(0);

    return offsetResponse.getConsumerOffsets().size() == TOPIC_COUNT;
  }
}
