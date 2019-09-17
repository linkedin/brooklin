/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaConnector;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig;
import com.linkedin.datastream.connectors.kafka.KafkaDatastreamStatesResponse;
import com.linkedin.datastream.connectors.kafka.LiKafkaConsumerFactory;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.connectors.kafka.PausedSourcePartitionMetadata;
import com.linkedin.datastream.connectors.kafka.TestKafkaConnectorUtils;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.DatastreamGroup;
import com.linkedin.datastream.server.DatastreamGroupPartitionsMetadata;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.FlushlessEventProducerHandler;
import com.linkedin.datastream.server.SourceBasedDeduper;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_PERIOD_MS;
import static com.linkedin.datastream.connectors.kafka.mirrormaker.KafkaMirrorMakerConnectorTestUtils.POLL_TIMEOUT_MS;


/**
 * Tests for {@link KafkaMirrorMakerConnector}
 */
@Test
public class TestKafkaMirrorMakerConnector extends BaseKafkaZkTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaMirrorMakerConnector.class);

  private static final String DATASTREAM_STATE_QUERY = "/datastream_state?datastream=";

  /**
   * Get the default config properties of a Kafka-based connector
   * @param override Configuration properties to override default config properties
   */
  public static Properties getDefaultConfig(Optional<Properties> override) {
    Properties config = new Properties();
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_KEY_SERDE, "keySerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_VALUE_SERDE, "valueSerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_COMMIT_INTERVAL_MILLIS, "10000");
    config.put(KafkaBasedConnectorConfig.CONFIG_COMMIT_TIMEOUT_MILLIS, "1000");
    config.put(KafkaBasedConnectorConfig.CONFIG_POLL_TIMEOUT_MILLIS, "5000");
    config.put(KafkaBasedConnectorConfig.CONFIG_CONSUMER_FACTORY_CLASS, LiKafkaConsumerFactory.class.getName());
    config.put(KafkaBasedConnectorConfig.CONFIG_PAUSE_PARTITION_ON_ERROR, Boolean.TRUE.toString());
    config.put(KafkaBasedConnectorConfig.CONFIG_RETRY_SLEEP_DURATION_MILLIS, "1000");
    config.put(KafkaBasedConnectorConfig.CONFIG_PAUSE_ERROR_PARTITION_DURATION_MILLIS,
        String.valueOf(Duration.ofSeconds(5).toMillis()));
    override.ifPresent(config::putAll);
    return config;
  }

  @Test
  public void testInitializeDatastream() throws Exception {
    String sourceRegex = "\\w+Event";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream ds =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastream", _broker, sourceRegex, metadata);
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("testInitializeDatastream", getDefaultConfig(Optional.empty()), "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());

    sourceRegex = "SpecificTopic";
    ds = KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastream2", _broker, sourceRegex,
        metadata);
    connector.initializeDatastream(ds, Collections.emptyList());

    sourceRegex = "(\\w+Event)|^(Topic)";
    ds = KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastream3", _broker, sourceRegex,
        metadata);
    connector.initializeDatastream(ds, Collections.emptyList());

    sourceRegex = "^(?!__)\\w+";
    ds = KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastream4", _broker, sourceRegex,
        metadata);
    connector.initializeDatastream(ds, Collections.emptyList());

    Assert.assertTrue(DatastreamUtils.isConnectorManagedDestination(ds));
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithDestinationReuse() throws DatastreamValidationException {
    String sourceRegex = "\\w+Event";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.TRUE.toString());
    Datastream ds =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastreamWithDestinationReuse", _broker,
            sourceRegex, metadata);
    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("testInitializeDatastreamWithDestinationReuse",
        getDefaultConfig(Optional.empty()), "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithBYOT() throws DatastreamValidationException {
    String sourceRegex = "\\w+Event";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());
    Datastream ds =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastreamWithBYOT", _broker, sourceRegex,
            metadata);
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("testInitializeDatastreamWithBYOT", getDefaultConfig(Optional.empty()),
            "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithBadSource() throws DatastreamValidationException {
    String sourceRegex = "*Event*";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream ds =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastreamWithBadSource", _broker,
            sourceRegex, metadata);
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("testInitializeDatastreamWithBadSource", getDefaultConfig(Optional.empty()),
            "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test
  public void testPopulateDatastreamDestination() throws Exception {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()), "testCluster");
    Coordinator coordinator =
        TestKafkaConnectorUtils.createCoordinator(_kafkaCluster.getZkConnection(), "testPopulateDatastreamDestination");
    coordinator.addConnector("KafkaMirrorMaker", connector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    String transportProviderName = "kafkaTransportProvider";
    KafkaTransportProviderAdmin transportProviderAdmin =
        TestKafkaConnectorUtils.createKafkaTransportProviderAdmin(_kafkaCluster);
    coordinator.addTransportProvider(transportProviderName, transportProviderAdmin);
    coordinator.start();

    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream stream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("testPopulateDatastreamDestination", _broker, "\\w+Event",
            metadata);
    stream.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(stream);

    String someTopic = "someTopic";
    Assert.assertEquals(transportProviderAdmin.getDestination(stream,  someTopic),
        stream.getDestination().getConnectionString().replace(KafkaMirrorMakerConnector.MM_TOPIC_PLACEHOLDER, someTopic));

    coordinator.stop();
  }

  @Test
  public void testValidateDatastreamUpdatePausedPartitions() throws Exception {
    String topic = "testValidateDatastreamUpdatePausedPartitions";
    Map<String, Set<String>> pausedPartitions = new HashMap<>();
    Map<String, Set<String>> expectedPartitions = new HashMap<>();

    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()), "testCluster");
    Coordinator coordinator = TestKafkaConnectorUtils.createCoordinator(_kafkaCluster.getZkConnection(),
        "testValidateDatastreamUpdatePausedPartitions");
    coordinator.addConnector("KafkaMirrorMaker", connector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    String transportProviderName = "kafkaTransportProvider";
    KafkaTransportProviderAdmin transportProviderAdmin =
        TestKafkaConnectorUtils.createKafkaTransportProviderAdmin(_kafkaCluster);
    coordinator.addTransportProvider(transportProviderName, transportProviderAdmin);
    coordinator.start();

    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("testPopulateDatastreamDestination", _broker, topic,
            metadata);
    datastream.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream);

    // create topic
    if (!AdminUtils.topicExists(_zkUtils, topic)) {
      AdminUtils.createTopic(_zkUtils, topic, 2, 1, new Properties(), null);
    }

    // Make sure "*" is converted to a list of partitions
    pausedPartitions.put(topic, new HashSet<>(Collections.singletonList("*")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "1")));
    verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions);

    // Make sure multiple *s and numbers is converted to a list of unique partitions
    pausedPartitions.put(topic, new HashSet<>(Arrays.asList("*", "2", "1", "*")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "1")));

    verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions);

    // Make sure numbers aren't touched
    pausedPartitions.put(topic, new HashSet<>(Collections.singletonList("1")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Collections.singletonList("1")));

    verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions);

    // Now add non-existent partition to list, and make sure it gets stripped off
    pausedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "99", "random", "1")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "1")));
    verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions);

    coordinator.stop();
  }

  @Test
  public void testMirrorMakerConnectorBasics() {
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";
    String saladTopic = "HealthySalad";

    createTopic(_zkUtils, saladTopic);
    createTopic(_zkUtils, yummyTopic);
    createTopic(_zkUtils, saltyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()), "testCluster");
    connector.start(null);

    // notify connector of new task
    connector.onAssignmentChange(Collections.singletonList(task));

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

    // manually pause partition for saltyTopic
    Map<String, Set<String>> pausedPartitions = new HashMap<>();
    pausedPartitions.put(saltyTopic, Sets.newHashSet("0"));
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));

    // notify connector of paused partition update
    connector.onAssignmentChange(Collections.singletonList(task));

    // verify manually paused partition from diagnostics
    if (!PollUtils.poll(() -> {
      Map<String, Set<String>> manuallyPaused =
          KafkaDatastreamStatesResponse.fromJson(connector.process(DATASTREAM_STATE_QUERY + datastream.getName()))
              .getManualPausedPartitions();
      return manuallyPaused.containsKey(saltyTopic) && manuallyPaused.get(saltyTopic).contains("0");
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("manualPausedPartitions was not properly retrieved from process() after pause");
    }

    // manually resume partition for saltyTopic
    pausedPartitions.remove(saltyTopic);
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));

    // notify connector of resumed partition update
    connector.onAssignmentChange(Collections.singletonList(task));

    if (!PollUtils.poll(() -> {
      Map<String, Set<String>> manuallyPaused =
          KafkaDatastreamStatesResponse.fromJson(connector.process(DATASTREAM_STATE_QUERY + datastream.getName()))
              .getManualPausedPartitions();
      return manuallyPaused.isEmpty();
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("manualPausedPartitions was not properly retrieved from process() after resume");
    }

    // simulate send error to auto-pause yummyTopic partition
    datastreamProducer.setSendFailCondition((record) -> true);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);

    if (!PollUtils.poll(() -> {
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions =
          KafkaDatastreamStatesResponse.fromJson(connector.process(DATASTREAM_STATE_QUERY + datastream.getName()))
              .getAutoPausedPartitions();
      TopicPartition expected = new TopicPartition(yummyTopic, 0);
      return autoPausedPartitions.containsKey(expected)
          && autoPausedPartitions.get(expected).getReason() == PausedSourcePartitionMetadata.Reason.SEND_ERROR;
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("autoPausedPartitions was not properly retrieved from process() after auto-pause");
    }

    // update the send fail condition to allow the message to successfully flow through message is retried
    datastreamProducer.setSendFailCondition((record) -> false);
    if (!PollUtils.poll(() -> {
      Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions =
          KafkaDatastreamStatesResponse.fromJson(connector.process(DATASTREAM_STATE_QUERY + datastream.getName()))
              .getAutoPausedPartitions();
      return autoPausedPartitions.isEmpty();
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("autoPausedPartitions was not properly retrieved from process() after auto-resume");
    }

    connector.stop();
  }

  @Test
  public void testStartConsumingFromNewTopic() {
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";

    createTopic(_zkUtils, yummyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    Properties override = new Properties();
    override.put(KafkaBasedConnectorConfig.DOMAIN_KAFKA_CONSUMER + "." + ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100");
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.of(override)), "testCluster");
    connector.start(null);

    // notify connector of new task
    connector.onAssignmentChange(Collections.singletonList(task));

    // produce an event to YummyPizza
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 1, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // verify the YummyPizza event was received
    DatastreamProducerRecord record = datastreamProducer.getEvents().get(0);
    Assert.assertEquals(record.getDestination().get(), yummyTopic,
        "Expected record from YummyPizza topic but got record from " + record.getDestination().get());

    // create a new topic ending in "Pizza" and produce an event to it
    createTopic(_zkUtils, saltyTopic);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // verify the SaltyPizza event was received
    record = datastreamProducer.getEvents().get(1);
    Assert.assertEquals(record.getDestination().get(), saltyTopic,
        "Expected record from SaltyPizza topic but got record from " + record.getDestination().get());

    connector.stop();
  }

  @Test
  public void testConsumptionUnaffectedByDeleteTopic() {
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";

    createTopic(_zkUtils, yummyTopic);
    createTopic(_zkUtils, saltyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    Properties override = new Properties();
    override.put(KafkaBasedConnectorConfig.DOMAIN_KAFKA_CONSUMER + "." + ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100");
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.of(override)), "testCluster");
    connector.start(null);

    // notify connector of new task
    connector.onAssignmentChange(Collections.singletonList(task));

    // produce an event to YummyPizza
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // clear the buffer for easier validation
    datastreamProducer.getEvents().clear();

    // delete the topic
    deleteTopic(_zkUtils, yummyTopic);

    // produce another event to SaltyPizza
    KafkaMirrorMakerConnectorTestUtils.produceEvents(saltyTopic, 1, _kafkaCluster);
    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 1, POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("did not get the event from SaltyPizza topic");
    }

    // verify the SaltyPizza event was received
    DatastreamProducerRecord record = datastreamProducer.getEvents().get(0);
    Assert.assertEquals(record.getDestination().get(), saltyTopic,
        "Expected record from SaltyPizza topic but got record from " + record.getDestination().get());

    connector.stop();
  }

  @Test
  public void testProcessDatastreamStatesRequestInvalid() {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()), "testCluster");

    connector.start(null);

    Assert.assertNull(connector.process(""));
    Assert.assertNull(connector.process("/not_datastream_state?datastream=name"));
    Assert.assertNull(connector.process("/datastream_state?notdatastream=name"));

    connector.stop();
  }

  @Test
  public void testReduceDatastreamStates() {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()), "testCluster");

    String datastreamName = "testProcessDatastreamStates";

    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";

    String instance1 = "instance1";
    String instance2 = "instance2";

    // build instance 1 results
    Set<TopicPartition> assignedPartitions1 = new HashSet<>();
    assignedPartitions1.add(new TopicPartition(yummyTopic, 0));
    assignedPartitions1.add(new TopicPartition(yummyTopic, 4));
    assignedPartitions1.add(new TopicPartition(yummyTopic, 10));
    assignedPartitions1.add(new TopicPartition(yummyTopic, 11));
    assignedPartitions1.add(new TopicPartition(yummyTopic, 23));
    assignedPartitions1.add(new TopicPartition(saltyTopic, 2));
    assignedPartitions1.add(new TopicPartition(saltyTopic, 5));
    assignedPartitions1.add(new TopicPartition(saltyTopic, 77));

    Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions1 = new HashMap<>();
    Map<String, Set<String>> manualPausedPartitions1 = new HashMap<>();

    autoPausedPartitions1.put(new TopicPartition(yummyTopic, 0),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));
    autoPausedPartitions1.put(new TopicPartition(yummyTopic, 10),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));

    manualPausedPartitions1.put(saltyTopic, Sets.newHashSet("2", "5", "77"));
    manualPausedPartitions1.put(yummyTopic, Sets.newHashSet("4", "11", "23"));

    Map<FlushlessEventProducerHandler.SourcePartition, Long> inflightMsgCounts1 = new HashMap<>();
    inflightMsgCounts1.put(new FlushlessEventProducerHandler.SourcePartition(yummyTopic, 4), 6L);

    KafkaDatastreamStatesResponse process1Response =
        new KafkaDatastreamStatesResponse(datastreamName, autoPausedPartitions1, manualPausedPartitions1,
            assignedPartitions1, inflightMsgCounts1);

    // build instance 2 results
    Set<TopicPartition> assignedPartitions2 = new HashSet<>();
    assignedPartitions2.add(new TopicPartition(yummyTopic, 19));
    assignedPartitions2.add(new TopicPartition(saltyTopic, 1));
    assignedPartitions2.add(new TopicPartition(saltyTopic, 6));
    assignedPartitions2.add(new TopicPartition(saltyTopic, 9));
    assignedPartitions2.add(new TopicPartition(saltyTopic, 17));
    assignedPartitions2.add(new TopicPartition(saltyTopic, 19));
    assignedPartitions2.add(new TopicPartition(saltyTopic, 25));

    Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions2 = new HashMap<>();
    Map<String, Set<String>> manualPausedPartitions2 = new HashMap<>();

    autoPausedPartitions2.put(new TopicPartition(saltyTopic, 6),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));
    autoPausedPartitions2.put(new TopicPartition(saltyTopic, 17),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));

    manualPausedPartitions2.put(saltyTopic, Sets.newHashSet("1", "9", "25"));
    manualPausedPartitions2.put(yummyTopic, Sets.newHashSet("19"));

    Map<FlushlessEventProducerHandler.SourcePartition, Long> inflightMsgCounts2 = new HashMap<>();
    inflightMsgCounts2.put(new FlushlessEventProducerHandler.SourcePartition(saltyTopic, 9), 20L);

    KafkaDatastreamStatesResponse process2Response =
        new KafkaDatastreamStatesResponse(datastreamName, autoPausedPartitions2, manualPausedPartitions2,
            assignedPartitions2, inflightMsgCounts2);

    Map<String, String> responseMap = new HashMap<>();
    responseMap.put(instance1, KafkaDatastreamStatesResponse.toJson(process1Response));
    responseMap.put(instance2, KafkaDatastreamStatesResponse.toJson(process2Response));

    String resultJson = connector.reduce(DATASTREAM_STATE_QUERY, responseMap);
    Map<String, String> reducedResults = JsonUtils.fromJson(resultJson, new TypeReference<HashMap<String, String>>() {
    });
    Assert.assertEquals(reducedResults.keySet().size(), 2, "Should have received states from 2 instances");

    // Validate instance1 states response
    KafkaDatastreamStatesResponse response1 = KafkaDatastreamStatesResponse.fromJson(responseMap.get(instance1));
    Assert.assertEquals(response1.getAssignedTopicPartitions(), assignedPartitions1,
        "Assigned partitions mismatch for instance1");
    Assert.assertEquals(response1.getInFlightMessageCounts(), inflightMsgCounts1,
        "In-flight message counts mismatch for instance1");
    Assert.assertEquals(response1.getManualPausedPartitions(), manualPausedPartitions1,
        "Manually paused partitions mismatch for instance1");
    Assert.assertEquals(response1.getAutoPausedPartitions(), autoPausedPartitions1,
        "Auto-paused partitions mismatch for instance1");
    Assert.assertEquals(response1.getDatastream(), datastreamName,
        "Datastream name mismatch for instance1");

    // Validate instance2 states response
    KafkaDatastreamStatesResponse response2 = KafkaDatastreamStatesResponse.fromJson(responseMap.get(instance2));
    Assert.assertEquals(response2.getAssignedTopicPartitions(), assignedPartitions2,
        "Assigned partitions mismatch for instance2");
    Assert.assertEquals(response2.getInFlightMessageCounts(), inflightMsgCounts2,
        "In-flight message counts mismatch for instance2");
    Assert.assertEquals(response2.getManualPausedPartitions(), manualPausedPartitions2,
        "Manually paused partitions mismatch for instance2");
    Assert.assertEquals(response2.getAutoPausedPartitions(), autoPausedPartitions2,
        "Auto-paused partitions mismatch for instance2");
    Assert.assertEquals(response2.getDatastream(), datastreamName,
        "Datastream name mismatch for instance");

    connector.stop();
  }

  @Test
  public void testFetchPartitionChange() throws Exception {
    String clusterName = "testGroupIdAssignment";
    Properties config = getDefaultConfig(Optional.empty());
    config.put(AbstractKafkaConnector.IS_GROUP_ID_HASHING_ENABLED, Boolean.toString(true));
    config.put(KafkaMirrorMakerConnector.PARTITION_FETCH_INTERVAL, "1000");
    config.put(KafkaBasedConnectorConfig.ENABLE_PARTITION_ASSIGNMENT, Boolean.toString(true));


    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("MirrorMakerConnector", config, clusterName);

    String yummyTopic = "YummyPizza";

    createTopic(_zkUtils, yummyTopic, 1);

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");
    DatastreamGroup group = new DatastreamGroup(ImmutableList.of(datastream));
    List<DatastreamGroup> datastreamGroups1 = new ArrayList<>();
    datastreamGroups1.add(group);

    AtomicInteger partitionChangeCalls = new AtomicInteger(0);
    connector.onPartitionChange(callbackGroup -> {
      if (callbackGroup.equals(group)) {
        partitionChangeCalls.incrementAndGet();
      }
    });

    connector.start(null);

    connector.handleDatastream(datastreamGroups1);
    Assert.assertTrue(PollUtils.poll(() -> partitionChangeCalls.get() == 1, POLL_PERIOD_MS, POLL_TIMEOUT_MS));
    Map<String, Optional<DatastreamGroupPartitionsMetadata>> partitionInfo = connector.getDatastreamPartitions();
    Assert.assertEquals(partitionInfo.get(group.getTaskPrefix()).get().getDatastreamGroup().getName(),
        group.getTaskPrefix());
    Assert.assertEquals(new HashSet<>(partitionInfo.get(group.getTaskPrefix()).get().getPartitions()),
        ImmutableSet.of(yummyTopic + "-0"));

    String saltyTopic = "SaltyPizza";
    createTopic(_zkUtils, saltyTopic, 2);

    Assert.assertTrue(PollUtils.poll(() -> partitionChangeCalls.get() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS));
    partitionInfo = connector.getDatastreamPartitions();
    Assert.assertEquals(new HashSet<>(partitionInfo.get(group.getTaskPrefix()).get().getPartitions()),
        ImmutableSet.of(yummyTopic + "-0", saltyTopic + "-0", saltyTopic + "-1"));
    connector.stop();
  }

  @Test
  public void testDisableDatastreamChangeListener() throws Exception {
    String clusterName = "testGroupIdAssignment";
    Properties config = getDefaultConfig(Optional.empty());
    config.put(KafkaBasedConnectorConfig.ENABLE_PARTITION_ASSIGNMENT, Boolean.toString(Boolean.FALSE));
    config.put(AbstractKafkaConnector.IS_GROUP_ID_HASHING_ENABLED, Boolean.toString(true));
    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("MirrorMakerConnector", config, clusterName);
    Datastream datastream1 =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("datastream1", _broker, "\\w+Event1");
    List<DatastreamGroup> datastreamGroups1 = new ArrayList<>();
    datastreamGroups1.add(new DatastreamGroup(ImmutableList.of(datastream1)));

    //subscribe callback
    connector.handleDatastream(datastreamGroups1);
    Map<String, Optional<DatastreamGroupPartitionsMetadata>> partitionInfo = connector.getDatastreamPartitions();

    Assert.assertEquals(partitionInfo.keySet(), Collections.EMPTY_SET);
  }

  @Test
  public void testChangeDatatstreamAssignment() throws Exception {
    String clusterName = "testGroupIdAssignment";
    Properties config = getDefaultConfig(Optional.empty());
    config.put(KafkaBasedConnectorConfig.ENABLE_PARTITION_ASSIGNMENT, Boolean.toString(true));
    config.put(AbstractKafkaConnector.IS_GROUP_ID_HASHING_ENABLED, Boolean.toString(true));
    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("MirrorMakerConnector", config, clusterName);
    Datastream datastream1 =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("datastream1", _broker, "\\w+Event1");
    Datastream datastream2 =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("datastream2", _broker, "\\w+Event2");


    List<DatastreamGroup> datastreamGroups1 = new ArrayList<>();
    datastreamGroups1.add(new DatastreamGroup(ImmutableList.of(datastream1)));

    //subscribe callback
    connector.onPartitionChange(t -> { });

    connector.handleDatastream(datastreamGroups1);

    Map<String, Optional<DatastreamGroupPartitionsMetadata>> partitionInfo = connector.getDatastreamPartitions();
    Assert.assertEquals(partitionInfo.keySet(),
        datastreamGroups1.stream().map(DatastreamGroup::getTaskPrefix).collect(Collectors.toSet()));

    // add datastream1
    List<DatastreamGroup> datastreamGroups2 = new ArrayList<>();
    datastreamGroups2.add(new DatastreamGroup(ImmutableList.of(datastream1)));
    datastreamGroups2.add(new DatastreamGroup(ImmutableList.of(datastream2)));

    connector.handleDatastream(datastreamGroups2);
    partitionInfo = connector.getDatastreamPartitions();

    Assert.assertEquals(partitionInfo.keySet(),
        datastreamGroups2.stream().map(DatastreamGroup::getTaskPrefix).collect(Collectors.toSet()));

    //remove datastream1
    List<DatastreamGroup> datastreamGroups3 = new ArrayList<>();
    datastreamGroups3.add(new DatastreamGroup(ImmutableList.of(datastream2)));

    connector.handleDatastream(datastreamGroups3);
    partitionInfo = connector.getDatastreamPartitions();

    Assert.assertEquals(partitionInfo.keySet(),
        datastreamGroups3.stream().map(DatastreamGroup::getTaskPrefix).collect(Collectors.toSet()));

  }

  @Test
  public void testGetStateForMultipleTasks() throws Exception {
    List<String> topics = Arrays.asList("YummyPizza", "SaltyPizza", "HealthySalad");
    topics.forEach(t -> createTopic(_zkUtils, t));

    // create a datastream to consume from topics ending in "Pizza"
    Datastream datastream = KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    // create three tasks for this datastream
    List<DatastreamTask> tasks = ImmutableList.of(new DatastreamTaskImpl(Collections.singletonList(datastream)),
        new DatastreamTaskImpl(Collections.singletonList(datastream)),
        new DatastreamTaskImpl(Collections.singletonList(datastream)));

    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    tasks.forEach(t -> ((DatastreamTaskImpl) t).setEventProducer(datastreamProducer));

    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()), "testCluster");
    connector.start(null);

    // notify connector of new task
    connector.onAssignmentChange(tasks);

    boolean hasExpectedTopicPartitions = PollUtils.poll(() -> {
      String jsonStr = connector.process("/datastream_state?datastream=pizzaStream");
      KafkaDatastreamStatesResponse response = KafkaDatastreamStatesResponse.fromJson(jsonStr);
      return response.getAssignedTopicPartitions().equals(ImmutableSet.of(new TopicPartition("SaltyPizza", 0),
          new TopicPartition("YummyPizza", 0)));
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Assert.assertTrue(hasExpectedTopicPartitions);
    connector.stop();
  }

  @Test
  public void testGroupIdAssignment() throws Exception {
    executeGroupIdAssignment(false);
  }

  @Test
  public void testGroupIdAssignmentWithHashing() throws Exception {
    executeGroupIdAssignment(true);
  }

  private void executeGroupIdAssignment(boolean groupIdHashingEnabled) throws Exception {
    String clusterName = "testGroupIdAssignment";
    Properties config = getDefaultConfig(Optional.empty());
    config.put(AbstractKafkaConnector.IS_GROUP_ID_HASHING_ENABLED, Boolean.toString(groupIdHashingEnabled));
    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("MirrorMakerConnector", config, clusterName);
    Coordinator coordinator = TestKafkaConnectorUtils.createCoordinator(_kafkaCluster.getZkConnection(), clusterName);
    coordinator.addConnector("KafkaMirrorMaker", connector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    String transportProviderName = "kafkaTransportProvider";
    KafkaTransportProviderAdmin transportProviderAdmin =
        TestKafkaConnectorUtils.createKafkaTransportProviderAdmin(_kafkaCluster);
    coordinator.addTransportProvider(transportProviderName, transportProviderAdmin);
    coordinator.start();

    // create datastream without any group ID specified in metadata, expect group ID constructed
    StringMap metadata1 = new StringMap();
    metadata1.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream datastream1 =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("datastream1", _broker, "\\w+Event", metadata1);
    datastream1.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream1);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream1);
    Assert.assertEquals(datastream1.getMetadata().get(DatastreamMetadataConstants.GROUP_ID),
        new KafkaMirrorMakerGroupIdConstructor(groupIdHashingEnabled, "testGroupIdAssignment").constructGroupId(
            datastream1));
    LOG.info("datastream1: {}", datastream1.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream1: {}", datastream1.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    // create datastream with group ID specified in metadata, expect that group ID being used as it is.
    StringMap metadata2 = new StringMap();
    metadata2.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    metadata2.put(DatastreamMetadataConstants.GROUP_ID, "randomId");
    Datastream datastream2 =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("datastream2", _broker, "\\w+Event", metadata2);
    datastream2.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream2);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream2);
    Assert.assertEquals(datastream2.getMetadata().get(DatastreamMetadataConstants.GROUP_ID), "randomId");
    LOG.info("datastream2: {}", datastream2.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream2: {}", datastream2.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    coordinator.stop();
  }

  private void verifyPausedPartitions(Connector connector, Datastream datastream,
      Map<String, Set<String>> pausedPartitions, Map<String, Set<String>> expectedPartitions) {
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));
    boolean validationSuccess = PollUtils.poll(() -> {
      try {
        connector.validateUpdateDatastreams(Collections.singletonList(datastream), Collections.singletonList(datastream));
        return expectedPartitions.equals(DatastreamUtils.getDatastreamSourcePartitions(datastream));
      } catch (Exception e) {
        LOG.warn("validateUpdateDatastreams failed with error: " + e);
        return false;
      }
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Assert.assertTrue(validationSuccess, "verifyPausedPartitions failed.");
  }
}
