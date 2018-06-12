package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Gauge;
import com.google.common.collect.Sets;

import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaBasedConnectorTask;
import com.linkedin.datastream.connectors.kafka.BaseKafkaZkTest;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorTaskMetrics;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.connectors.kafka.KafkaDatastreamStatesResponse;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.kafka.KafkaDatastreamMetadataConstants;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;

import static com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorTaskMetrics.*;
import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS;
import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS;

public class TestKafkaMirrorMakerConnectorTask extends BaseKafkaZkTest {

  private static final long CONNECTOR_AWAIT_STOP_TIMEOUT_MS = 30000;
  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaMirrorMakerConnectorTask.class);

  @Test
  public void testConsumeFromMultipleTopics() throws Exception {
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";
    String saladTopic = "HealthySalad";

    createTopic(_zkUtils, saladTopic);
    createTopic(_zkUtils, yummyTopic);
    createTopic(_zkUtils, saltyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(task);
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // produce an event to each of the 3 topics
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saladTopic, 1, _kafkaCluster);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    List<DatastreamProducerRecord> records = datastreamProducer.getEvents();
    for (DatastreamProducerRecord record : records) {
      String destinationTopic = record.getDestination().get();
      Assert.assertTrue(destinationTopic.endsWith("Pizza"),
          "Unexpected event consumed from Datastream and sent to topic: " + destinationTopic);
    }

    // verify the states response returned by diagnostics endpoint contains correct counts
    validateTaskConsumerAssignment(connectorTask,
        Sets.newHashSet(new TopicPartition(yummyTopic, 0), new TopicPartition(saltyTopic, 0)));

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testFlushAndCommitDuringGracefulStop() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    // create a producer that will take a few seconds to flush
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer(null, null, Duration.ofSeconds(2));
    task.setEventProducer(datastreamProducer);

    // create a task that checkpoints very infrequently (10 minutes for purposes of this test)
    KafkaMirrorMakerConnectorTask connectorTask = new KafkaMirrorMakerConnectorTask(
        new KafkaBasedConnectorConfig(new KafkaConsumerFactoryImpl(), null, new Properties(), "", "", 600000, 5,
            Duration.ofSeconds(0), false, Duration.ofSeconds(0)), task, "", false);
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(task);
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // produce events to the topic
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 2, _kafkaCluster);

    // verify the events were read
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // stop the task
    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");

    // verify that flush was called on the producer
    Assert.assertEquals(datastreamProducer.getNumFlushes(), 1);
  }

  @Test
  public void testAutoOffsetResetConfigOverride() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create 2 datastreams to consume from topics ending in "Pizza", each with different offset reset strategy
    Datastream datastreamLatest =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");
    datastreamLatest.getMetadata().put(KafkaDatastreamMetadataConstants.CONSUMER_OFFSET_RESET_STRATEGY,
        AbstractKafkaBasedConnectorTask.CONSUMER_AUTO_OFFSET_RESET_CONFIG_LATEST);
    datastreamLatest.getMetadata().put(DatastreamMetadataConstants.GROUP_ID, "groupIdForDatastreamLatest");

    Datastream datastreamEarliest =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");
    datastreamEarliest.getMetadata().put(KafkaDatastreamMetadataConstants.CONSUMER_OFFSET_RESET_STRATEGY,
        AbstractKafkaBasedConnectorTask.CONSUMER_AUTO_OFFSET_RESET_CONFIG_EARLIEST);
    datastreamEarliest.getMetadata().put(DatastreamMetadataConstants.GROUP_ID, "groupIdForDatastreamEarliest");

    // produce some events to YummyPizza topic
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    // create datastream tasks for the datastreams
    DatastreamTaskImpl taskLatest = new DatastreamTaskImpl(Collections.singletonList(datastreamLatest));
    MockDatastreamEventProducer datastreamProducerLatest = new MockDatastreamEventProducer();
    taskLatest.setEventProducer(datastreamProducerLatest);

    DatastreamTaskImpl taskEarliest = new DatastreamTaskImpl(Collections.singletonList(datastreamEarliest));
    MockDatastreamEventProducer datastreamProducerEarliest = new MockDatastreamEventProducer();
    taskEarliest.setEventProducer(datastreamProducerEarliest);

    // create connector tasks for the datastream tasks and run them
    KafkaMirrorMakerConnectorTask connectorTaskLatest =
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(taskLatest);
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTaskLatest);

    KafkaMirrorMakerConnectorTask connectorTaskEarliest =
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(taskEarliest);
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTaskEarliest);

    // verify that the datastream configured for earliest got the events, while the one configured for latest got 0
    Assert.assertTrue(PollUtils.poll(
        () -> datastreamProducerEarliest.getEvents().size() == 5 && datastreamProducerLatest.getEvents().size() == 0,
        POLL_PERIOD_MS, POLL_TIMEOUT_MS), "Datastream configured with offset reset latest should have gotten "
        + "0 events, and datastream configured with earliest should have gotten 5 events");

    // produce more events to YummyPizza topic
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    Assert.assertTrue(PollUtils.poll(
        () -> datastreamProducerEarliest.getEvents().size() == 10 && datastreamProducerLatest.getEvents().size() == 5,
        POLL_PERIOD_MS, POLL_TIMEOUT_MS), "Datastream configured with offset reset latest should have gotten "
        + "5 events, and datastream configured with earliest should have gotten 10 events");

    connectorTaskLatest.stop();
    Assert.assertTrue(connectorTaskLatest.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
    connectorTaskEarliest.stop();
    Assert.assertTrue(connectorTaskEarliest.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testConfigPauseAndResumePartitions() throws Exception {
    // Need connector just for update validation. Doesn't matter properties or datastream name
    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("foo", new Properties());

    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";
    String spicyTopic = "SpicyPizza";

    createTopic(_zkUtils, spicyTopic);
    createTopic(_zkUtils, yummyTopic);
    createTopic(_zkUtils, saltyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl datastreamTask = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    datastreamTask.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(datastreamTask);
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // Make sure there was one initial update
    if (!PollUtils.poll(() -> connectorTask.getPausedPartitionsConfigUpdateCount() == 1, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated at the beginning. Expecting update count 1, found: "
          + connectorTask.getPausedPartitionsConfigUpdateCount());
    }

    // Make sure there isn't any paused partition
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig().size(), 0);
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 0, 0, 0);


    // Produce an event to each of the 3 topics
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(spicyTopic, 1, _kafkaCluster);

    // Make sure all 3 events were read.
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 3, 100, 25000)) {
      Assert.fail(
          "Did not transfer the msgs within timeout. Expected: 3 Tansferred: " + datastreamProducer.getEvents().size());
    }

    // Now create paused partitions
    // yummypizza - with partition 0
    // spicypizza - with all partitions ("*")
    Map<String, HashSet<String>> pausedPartitions = new HashMap<>();
    Map<String, HashSet<String>> expectedPartitions = new HashMap<>();
    pausedPartitions.put(yummyTopic, new HashSet<>(Collections.singletonList("0")));
    pausedPartitions.put(spicyTopic, new HashSet<>(Collections.singletonList("*")));
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));

    // Update connector task with paused partitions
    connector.validateUpdateDatastreams(Collections.singletonList(datastream), Collections.singletonList(datastream));
    connectorTask.checkForUpdateTask(datastreamTask);

    // Make sure there was an update, and that there paused partitions.
    if (!PollUtils.poll(() -> connectorTask.getPausedPartitionsConfigUpdateCount() == 2, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated. Expecting update count 2, found: "
          + connectorTask.getPausedPartitionsConfigUpdateCount());
    }

    // Make sure the paused partitions match.
    // prepare expectedPartitions for match
    expectedPartitions.put(yummyTopic, new HashSet<>(Collections.singletonList("0")));
    expectedPartitions.put(spicyTopic, new HashSet<>(Collections.singletonList("0")));
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig().size(), 2);
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 0, 0, 2);
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig(), expectedPartitions);

    // Produce an event to each of the 3 topics
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(spicyTopic, 1, _kafkaCluster);

    // Make sure only 1 event was seen
    // Note: the mock producer doesn't delete previous messages by default, so the previously read records should also
    // be there.
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 4, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail(
          "Did not transfer the msgs within timeout. Expected: 4 Tansferred: " + datastreamProducer.getEvents().size());
    }

    // Now pause same set of partitions, and make sure there isn't any update.
    connectorTask.checkForUpdateTask(datastreamTask);
    if (!PollUtils.poll(() -> connectorTask.getPausedPartitionsConfigUpdateCount() == 2, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated. Expecting update count 2, found: "
          + connectorTask.getPausedPartitionsConfigUpdateCount());
    }
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig().size(), 2);
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 0, 0, 2);
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig(), expectedPartitions);
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 4, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail(
          "Transferred msgs when not expected. Expected: 4 Tansferred:  " + datastreamProducer.getEvents().size());
    }


    // Now add * to yummypizza and 0 to spicy pizza, and make sure 0 is neglected for spicypizza and a * is added for yummypizza
    // As * will translate to all partitions, and yummypizza has only 1 partition which is already added, this will be a noop
    pausedPartitions.clear();
    pausedPartitions.put(yummyTopic, new HashSet<>(Collections.singletonList("*")));
    pausedPartitions.put(spicyTopic, new HashSet<>(Collections.singletonList("0")));
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));
    connector.validateUpdateDatastreams(Collections.singletonList(datastream), Collections.singletonList(datastream));
    connectorTask.checkForUpdateTask(datastreamTask);
    if (!PollUtils.poll(() -> connectorTask.getPausedPartitionsConfigUpdateCount() == 2, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated. Expecting update count 2, found: "
          + connectorTask.getPausedPartitionsConfigUpdateCount());
    }
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig().size(), 2);
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 0, 0, 2);
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig(), expectedPartitions);
    // Make sure other 1 extra event was read
    // Note: the mock producer doesn't delete previous messages by default, so the previously read records should also
    // be there.
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 4, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("Transferred messages when not expected, after * partitions were added. Expected: 4 Transferred: "
          + datastreamProducer.getEvents().size());
    }


    // Now update partition assignment
    // Doesn't matter the partition/topic - we just want to ensure paused partitions are updated (to the same value)
    connectorTask.onPartitionsAssigned(Collections.singletonList(new TopicPartition("randomTopic", 0)));
    if (!PollUtils.poll(() -> connectorTask.getPausedPartitionsConfigUpdateCount() == 3, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated. Expecting update count 3, found: "
          + connectorTask.getPausedPartitionsConfigUpdateCount());
    }
    // Expect no change in paused partitions
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig(), expectedPartitions);
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 0, 0, 2);

    // Now resume both the partitions.
    datastream.getMetadata().put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, "");
    connector.validateUpdateDatastreams(Collections.singletonList(datastream), Collections.singletonList(datastream));
    connectorTask.checkForUpdateTask(datastreamTask);
    if (!PollUtils.poll(() -> connectorTask.getPausedPartitionsConfigUpdateCount() == 4, POLL_PERIOD_MS,
        POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated. Expecting update count 4, found: "
          + connectorTask.getPausedPartitionsConfigUpdateCount());
    }
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig().size(), 0);
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 0, 0, 0);
    // Prepare expectedPartitions
    expectedPartitions.clear();
    Assert.assertEquals(connectorTask.getPausedPartitionsConfig(), expectedPartitions);
    // Make sure other 2 events were read
    // Note: the mock producer doesn't delete previous messages by default, so the previously read records should also
    // be there.
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 6, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail(
          "Did not transfer the msgs within timeout. Expected: 6 Tansferred: " + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testAutoPauseOnSendFailure() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    // create event producer that fails on 3rd event (of 5)
    MockDatastreamEventProducer datastreamProducer =
        new MockDatastreamEventProducer((r) -> new String((byte[]) r.getEvents().get(0).key().get()).equals("key-2"));
    task.setEventProducer(datastreamProducer);

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(task, consumerProps,
            Duration.ofSeconds(20));
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // produce 5 events
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    // validate that the topic partition was added to auto-paused set
    if (!PollUtils.poll(() -> connectorTask.getAutoPausedSourcePartitions().contains(new TopicPartition(yummyTopic, 0)),
        POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("Partition did not auto-pause after multiple send failures.");
    }

    Assert.assertEquals(datastreamProducer.getEvents().size(), 2,
        "The events before the failure should have been sent");

    // resume the auto-paused partition by manually pausing and resuming
    // pause the partition
    Map<String, Set<String>> pausedPartitions = new HashMap<>();
    pausedPartitions.put(yummyTopic, new HashSet<>(Collections.singletonList("0")));
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));
    task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    connectorTask.checkForUpdateTask(task);

    if (!PollUtils.poll(() -> pausedPartitions.equals(connectorTask.getPausedPartitionsConfig())
        && connectorTask.getAutoPausedSourcePartitions().isEmpty(), POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated after adding partitions to pause config.");
    }

    // update the send failure condition so that events flow through once partition is resumed
    datastreamProducer.updateSendFailCondition((r) -> false);

    // resume the partition
    pausedPartitions.put(yummyTopic, Collections.emptySet());
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));
    task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    connectorTask.checkForUpdateTask(task);

    if (!PollUtils.poll(() -> pausedPartitions.equals(connectorTask.getPausedPartitionsConfig()), POLL_PERIOD_MS,
        POLL_TIMEOUT_MS)) {
      Assert.fail("Paused partitions were not updated after removing partitions from pause config.");
    }

    // verify that all the events got sent (the first 2 events got sent twice)
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 7, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("datastream producer " + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testAutoPauseAndResumeOnSendFailure() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    // create event producer that fails on 3rd event (of 5)
    MockDatastreamEventProducer datastreamProducer =
        new MockDatastreamEventProducer((r) -> new String((byte[]) r.getEvents().get(0).key().get()).equals("key-2"));
    task.setEventProducer(datastreamProducer);

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    KafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(task, consumerProps, Duration.ofSeconds(5));
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // produce 5 events
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    // validate that the topic partition was added to auto-paused set
    if (!PollUtils.poll(() -> connectorTask.getAutoPausedSourcePartitions().contains(new TopicPartition(yummyTopic, 0)),
        POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("Partition did not auto-pause after multiple send failures.");
    }

    Assert.assertEquals(datastreamProducer.getEvents().size(), 2,
        "The events before the failure should have been sent");

    // validate metrics
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 1, 0, 0);

    // update the send failure condition so that events flow through once partition is resumed
    datastreamProducer.updateSendFailCondition((r) -> false);

    Assert.assertTrue(
        PollUtils.poll(() -> connectorTask.getAutoPausedSourcePartitions().isEmpty(), POLL_PERIOD_MS, POLL_TIMEOUT_MS),
        "Partition that was auto-paused did not auto-resume.");

    // verify that all the events got sent (the first 2 events got sent twice)
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 7, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("datastream producer " + datastreamProducer.getEvents().size());
    }

    // validate metrics
    validatePausedPartitionsMetrics("KafkaMirrorMakerConnectorTask", datastream.getName(), 0, 0, 0);

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testDeleteSourceTopic() throws Exception {
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";

    createTopic(_zkUtils, yummyTopic);
    createTopic(_zkUtils, saltyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createKafkaMirrorMakerConnectorTask(task);
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // verify the task is subscribed to both topics
    validateTaskConsumerAssignment(connectorTask,
        Sets.newHashSet(new TopicPartition(yummyTopic, 0), new TopicPartition(saltyTopic, 0)));

    // produce an event to each of the topics
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 100, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);

    // delete YummyPizza topic
    deleteTopic(_zkUtils, yummyTopic);

    // verify the task is no longer subscribed to the deleted topic YummyPizza
    boolean partitionRevoked = PollUtils.poll(() -> {
          Set<TopicPartition> assigned = connectorTask.getKafkaDatastreamStatesResponse().getAssignedTopicPartitions();
          return assigned.size() == 1 && assigned.contains(new TopicPartition(saltyTopic, 0));
        }, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    Assert.assertTrue(partitionRevoked, "The deleted topic should have been revoked, but is still assigned");

    // produce another event to SaltyPizza
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);

    // verify that 2 events produced to SaltyPizza were received
    if (!PollUtils.poll(() -> datastreamProducer.getEvents()
        .stream()
        .filter(record -> record.getDestination().get().endsWith(saltyTopic))
        .count() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testMirrorMakerGroupId() throws Exception {
    Datastream datastream1 = KafkaMirrorMakerConnectorTestUtils.createDatastream("datastream1", _broker, "topic");
    Datastream datastream2 = KafkaMirrorMakerConnectorTestUtils.createDatastream("datastream2", _broker, "topic");

    // This situation (multiple datastreams in MM task) in theory shouldn't be there for MM (as it doesn't allow duplication)
    // Creating the task with multiple datastreams strictly for testing purposes.
    DatastreamTaskImpl task = new DatastreamTaskImpl(Arrays.asList(datastream1, datastream2));
    CommonConnectorMetrics consumerMetrics =
        new CommonConnectorMetrics(TestKafkaMirrorMakerConnectorTask.class.getName(), "testConsumer", LOG);
    consumerMetrics.createEventProcessingMetrics();

    String defaultGrpId = datastream1.getName();

    // Testing with default group id
    Assert.assertEquals(KafkaMirrorMakerConnectorTask.getMirrorMakerGroupId(task, consumerMetrics, LOG), defaultGrpId);

    // Test with setting explicit group id in one datastream
    datastream1.getMetadata().put(ConsumerConfig.GROUP_ID_CONFIG, "MyGroupId");
    Assert.assertEquals(KafkaMirrorMakerConnectorTask.getMirrorMakerGroupId(task, consumerMetrics, LOG), "MyGroupId");

    // Test with explicitly setting group id in both datastream
    datastream2.getMetadata().put(ConsumerConfig.GROUP_ID_CONFIG, "MyGroupId");
    Assert.assertEquals(KafkaMirrorMakerConnectorTask.getMirrorMakerGroupId(task, consumerMetrics, LOG), "MyGroupId");

    // now set different group ids in 2 datastreams and make sure validation fails
    datastream2.getMetadata().put(ConsumerConfig.GROUP_ID_CONFIG, "invalidGroupId");
    boolean exceptionSeen = false;
    try {
      KafkaMirrorMakerConnectorTask.getMirrorMakerGroupId(task, consumerMetrics, LOG);
    } catch (DatastreamRuntimeException e) {
      exceptionSeen = true;
    }
    Assert.assertTrue(exceptionSeen);
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testAutoPauseAndResume() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer(Duration.ofMillis(500));
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask =
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
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 5, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
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
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testFlowControlDisabled() throws Exception {
    String yummyTopic = "YummyPizza";
    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer(Duration.ofMillis(500));
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask =
        KafkaMirrorMakerConnectorTestUtils.createFlushlessKafkaMirrorMakerConnectorTask(task, false, 2, 4,
            Duration.ofSeconds(0));
    KafkaMirrorMakerConnectorTestUtils.runKafkaMirrorMakerConnectorTask(connectorTask);

    // verify there are no paused partitions
    Assert.assertTrue(connectorTask.getAutoPausedSourcePartitions().isEmpty(),
        "auto-paused source partitions set should have been empty.");

    // produce 5 events
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 5, _kafkaCluster);

    // verify that the 5 events were sent
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 5, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // verify that flow control was never triggered
    Assert.assertEquals(connectorTask.getFlowControlTriggerCount(), 0,
        "Flow control should not have been triggered, as feature is disabled.");

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
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
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer((r) -> true);
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask =
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
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  private void validateTaskConsumerAssignment(KafkaMirrorMakerConnectorTask connectorTask,
      Set<TopicPartition> expectedAssignment) {
    KafkaDatastreamStatesResponse response = connectorTask.getKafkaDatastreamStatesResponse();
    Set<TopicPartition> assignedTopicPartitions = response.getAssignedTopicPartitions();
    if (expectedAssignment.isEmpty()) {
      Assert.assertTrue(assignedTopicPartitions == null || assignedTopicPartitions.isEmpty(),
          "There should have been no assigned topic partitions");
    } else {
      Assert.assertEquals(assignedTopicPartitions.size(), expectedAssignment.size(),
          "Topic partition assignment count is wrong");
      expectedAssignment.forEach(tp -> Assert.assertTrue(assignedTopicPartitions.contains(tp),
          "Assigned topic partitions in diagnostics response should have contained  " + tp));
    }
  }

  private void validatePausedPartitionsMetrics(String task, String stream, long numAutoPausedPartitionsOnError,
      long numAutoPausedPartitionsOnInFlightMessages, long numConfigPausedPartitions) {
    Assert.assertTrue(PollUtils.poll(() -> (long) ((Gauge) DynamicMetricsManager.getInstance()
            .getMetric(String.join(".", task, stream,
                KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_ERROR))).getValue()
            == numAutoPausedPartitionsOnError, POLL_PERIOD_MS, POLL_TIMEOUT_MS),
        "numAutoPausedPartitionsOnError metric failed to update");
    Assert.assertTrue(PollUtils.poll(() -> (long) ((Gauge) DynamicMetricsManager.getInstance()
            .getMetric(String.join(".", task, stream,
                KafkaBasedConnectorTaskMetrics.NUM_AUTO_PAUSED_PARTITIONS_ON_INFLIGHT_MESSAGES))).getValue()
            == numAutoPausedPartitionsOnInFlightMessages, POLL_PERIOD_MS, POLL_TIMEOUT_MS),
        "numAutoPausedPartitionsOnInFlightMessages metric failed to update");
    Assert.assertTrue(PollUtils.poll(() -> (long) ((Gauge) DynamicMetricsManager.getInstance()
            .getMetric(
                String.join(".", task, stream, KafkaBasedConnectorTaskMetrics.NUM_CONFIG_PAUSED_PARTITIONS))).getValue()
            == numConfigPausedPartitions, POLL_PERIOD_MS, POLL_TIMEOUT_MS),
        "numConfigPausedPartitions metric failed to update");
  }
}
