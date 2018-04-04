package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Gauge;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.kafka.BaseKafkaZkTest;
import com.linkedin.datastream.connectors.kafka.KafkaDatastreamStatesResponse;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;

import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorTaskMetrics.*;
import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS;
import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS;


public class TestFlushlessKafkaMirrorMakerConnectorTask extends BaseKafkaZkTest {

  private static final long CONNECTOR_AWAIT_STOP_TIMEOUT_MS = 30000;

  @Test
  @SuppressWarnings("unchecked")
  public void testAutoPauseAndResume() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer(Duration.ofMillis(500));
    task.setEventProducer(datastreamProducer);

    FlushlessKafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createFlushlessKafkaMirrorMakerConnectorTask(task, true, 2, 4,
            Duration.ofSeconds(0));
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // verify there are no paused partitions
    Assert.assertTrue(connectorTask.getAutoPausedSourcePartitions().isEmpty(),
        "auto-paused source partitions set should have been empty.");

    // produce 5 events
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    // verify that the partition was auto-paused due to inflight-message count exceeding 4
    Assert.assertTrue(PollUtils.poll(() -> {
      Gauge<Long> metric = DynamicMetricsManager.getInstance()
          .getMetric(KafkaMirrorMakerConnectorTask.class.getSimpleName() + ".pizzaStream."
              + NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES);
      return connectorTask.getAutoPausedSourcePartitions().size() == 1 && metric.getValue().equals(Long.valueOf(1));
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS), "partition should have been auto-paused after sending 5 messages");
    // verify that flow control was triggered
    Assert.assertEquals(connectorTask.getFlowControlTriggerCount(), 1, "Flow control should have been triggered");

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
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS), "did not shut down on time");
  }

  @Test
  public void testFlowControlDisabled() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer(Duration.ofMillis(500));
    task.setEventProducer(datastreamProducer);

    FlushlessKafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createFlushlessKafkaMirrorMakerConnectorTask(task, false, 2, 4,
            Duration.ofSeconds(0));
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // verify there are no paused partitions
    Assert.assertTrue(connectorTask.getAutoPausedSourcePartitions().isEmpty(),
        "auto-paused source partitions set should have been empty.");

    // produce 5 events
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    // verify that the 5 events were sent
    if (!PollUtils.poll(
        () -> datastreamProducer.getEvents().size() == 5, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // verify that flow control was never triggered
    Assert.assertEquals(connectorTask.getFlowControlTriggerCount(), 0, "Flow control should not have been triggered, as feature is disabled.");

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS), "did not shut down on time");
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
        KafkaMirrorMakerConnectorTestUtils.createFlushlessKafkaMirrorMakerConnectorTask(task, true, 50, 100,
            Duration.ofDays(1));
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
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS), "did not shut down on time");
  }
}
