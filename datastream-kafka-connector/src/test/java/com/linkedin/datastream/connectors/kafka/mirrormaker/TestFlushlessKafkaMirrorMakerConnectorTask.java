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
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;

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
        KafkaMirrorMakerConnectorTestUtils.createFlushlessKafkaMirrorMakerConnectorTask(task, new Properties(), 2, 4);
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
    Assert.assertTrue(connectorTask.awaitStop(5000, TimeUnit.MILLISECONDS), "did not shut down on time");
  }
}
