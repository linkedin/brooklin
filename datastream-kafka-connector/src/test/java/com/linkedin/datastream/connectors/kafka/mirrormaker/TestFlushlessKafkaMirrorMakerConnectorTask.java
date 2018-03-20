package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.kafka.BaseKafkaZkTest;
import com.linkedin.datastream.connectors.kafka.KafkaDatastreamStatesResponse;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;

import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS;
import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS;


public class TestFlushlessKafkaMirrorMakerConnectorTask extends BaseKafkaZkTest {

  @Test
  public void testAutoPauseAndResume() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer(Duration.ofMillis(500));
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createFlushlessKafkaMirrorMakerConnectorTask(task, new Properties(), 0, 2, 4,
            true, Duration.ofSeconds(0));
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);


    // verify there are no paused partitions
    Assert.assertTrue(connectorTask.getAutoPausedSourcePartitions().isEmpty(),
        "auto-paused source partitions set should have been empty.");

    // produce 5 events
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    // verify that the partition was auto-paused due to inflight-message count exceeding 4
    Assert.assertTrue(PollUtils.poll(() -> connectorTask.getAutoPausedSourcePartitions().size() == 1, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS), "partition should have been auto-paused after sending 5 messages");

    // verify that the 5 events were eventually sent
    if (!PollUtils.poll(
        () -> datastreamProducer.getEvents().size() == 5, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // partition should be paused, so produce another batch and verify that partition was auto-resumed
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);
    if (!PollUtils.poll(
        () -> datastreamProducer.getEvents().size() == 10 && connectorTask.getAutoPausedSourcePartitions().size() == 0,
        POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not auto-resume and transfer the remaining msgs within timeout. transferred "
          + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(10000, TimeUnit.MILLISECONDS), "did not shut down on time");
  }

  @Test
  public void testInFlightMessageCount() throws Exception {
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";
    String spicyTopic = "SpicyPizza";
    createTopic(_zkUtils, yummyTopic);
    createTopic(_zkUtils, saltyTopic);
    createTopic(_zkUtils, spicyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer((r) -> true);
    task.setEventProducer(datastreamProducer);

    FlushlessKafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createFlushlessKafkaMirrorMakerConnectorTask(task, new Properties(), 0, 50, 100,
            true, Duration.ofDays(1));
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // produce events to each topic
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 2, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(spicyTopic, 1, _kafkaCluster);

    // verify that in-flight message count for each topic is 1
    Assert.assertTrue(PollUtils.poll(() -> connectorTask.getInFlightMessagesCount(yummyTopic, 0) == 1, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS),
        "yummyTopic should have in-flight message count of 1 but was: " + connectorTask.getInFlightMessagesCount(
            yummyTopic, 0));
    Assert.assertTrue(PollUtils.poll(() -> connectorTask.getInFlightMessagesCount(saltyTopic, 0) == 1, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS),
        "saltyTopic should have in-flight message count of 1 but was: " + connectorTask.getInFlightMessagesCount(
            saltyTopic, 0));
    Assert.assertTrue(PollUtils.poll(() -> connectorTask.getInFlightMessagesCount(spicyTopic, 0) == 1, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS),
        "spicyTopic should have in-flight message count of 1 but was: " + connectorTask.getInFlightMessagesCount(
            spicyTopic, 0));

    // verify the states response returned by diagnostics endpoint contains correct counts
    KafkaDatastreamStatesResponse statesResponse = connectorTask.getKafkaDatastreamStatesResponse();
    Assert.assertEquals(statesResponse.getAutoPausedPartitions().size(), 3,
        "All topics should have had auto-paused partitions");
    Assert.assertEquals(
        statesResponse.getInFlightMessageCounts().get(new FlushlessEventProducerHandler.SourcePartition(yummyTopic, 0)),
        Long.valueOf(1), "In flight message count for yummyTopic was incorrect");
    Assert.assertEquals(
        statesResponse.getInFlightMessageCounts().get(new FlushlessEventProducerHandler.SourcePartition(saltyTopic, 0)),
        Long.valueOf(1), "In flight message count for yummyTopic was incorrect");
    Assert.assertEquals(
        statesResponse.getInFlightMessageCounts().get(new FlushlessEventProducerHandler.SourcePartition(spicyTopic, 0)),
        Long.valueOf(1), "In flight message count for yummyTopic was incorrect");

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(10000, TimeUnit.MILLISECONDS), "did not shut down on time");
  }
}
