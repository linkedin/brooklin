package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.IntStream;

import org.codehaus.jackson.type.TypeReference;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerOffsetsResponse;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;


@Test
public class TestKafkaConsumerOffsets extends BaseKafkaZkTest {

  private static final int TOPIC_COUNT = 2;
  private static final int PARTITION_COUNT = 2;
  private static final int PARTITION_MESSAGE_COUNT = 10;

  private static final String CONSUMER_OFFSETS = "/consumer_offsets";

  @Test
  public void testConsumerOffsetsDiagEndpoint() throws Exception {
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
