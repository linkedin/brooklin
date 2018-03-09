package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Sets;
import kafka.admin.AdminUtils;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.kafka.BaseKafkaZkTest;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.connectors.kafka.KafkaDatastreamStatesResponse;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.connectors.kafka.PausedSourcePartitionMetadata;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.connector.Connector;
import com.linkedin.datastream.server.CachedDatastreamReader;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.CoordinatorConfig;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.DummyTransportProviderAdminFactory;
import com.linkedin.datastream.server.SourceBasedDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.*;


@Test
public class TestKafkaMirrorMakerConnector extends BaseKafkaZkTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaMirrorMakerConnector.class);
  private static final long POLL_TIMEOUT_MS = Duration.ofSeconds(25).toMillis();
  private static final long POLL_PERIOD_MS = Duration.ofMillis(100).toMillis();

  private static final String DATASTREAM_STATE_QUERY = "/datastream_state?datastream=";

  private CachedDatastreamReader _cachedDatastreamReader;

  private Properties getDefaultConfig(Optional<Properties> override) {
    Properties config = new Properties();
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_KEY_SERDE, "keySerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_VALUE_SERDE, "valueSerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_COMMIT_INTERVAL_MILLIS, "10000");
    config.put(KafkaBasedConnectorConfig.CONFIG_CONSUMER_FACTORY_CLASS, KafkaConsumerFactoryImpl.class.getName());
    config.put(KafkaBasedConnectorConfig.CONFIG_PAUSE_PARTITION_ON_ERROR, Boolean.TRUE.toString());
    config.put(KafkaBasedConnectorConfig.CONFIG_RETRY_SLEEP_DURATION_MS, "1000");
    config.put(KafkaBasedConnectorConfig.CONFIG_PAUSE_ERROR_PARTITION_DURATION_MS,
        String.valueOf(Duration.ofSeconds(5).toMillis()));
    override.ifPresent(o -> config.putAll(o));
    return config;
  }


  private Coordinator createCoordinator(String zkAddr, String cluster) throws Exception {
    return createCoordinator(zkAddr, cluster, new Properties());
  }

  private Coordinator createCoordinator(String zkAddr, String cluster, Properties override) throws Exception {
    Properties props = new Properties();
    props.put(CoordinatorConfig.CONFIG_CLUSTER, cluster);
    props.put(CoordinatorConfig.CONFIG_ZK_ADDRESS, zkAddr);
    props.put(CoordinatorConfig.CONFIG_ZK_SESSION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_SESSION_TIMEOUT));
    props.put(CoordinatorConfig.CONFIG_ZK_CONNECTION_TIMEOUT, String.valueOf(ZkClient.DEFAULT_CONNECTION_TIMEOUT));
    props.putAll(override);
    ZkClient client = new ZkClient(zkAddr);
    _cachedDatastreamReader = new CachedDatastreamReader(client, cluster);
    Coordinator coordinator = new Coordinator(_cachedDatastreamReader, props);
    DummyTransportProviderAdminFactory factory = new DummyTransportProviderAdminFactory();
    coordinator.addTransportProvider(DummyTransportProviderAdminFactory.PROVIDER_NAME,
        factory.createTransportProviderAdmin(DummyTransportProviderAdminFactory.PROVIDER_NAME, new Properties()));
    return coordinator;
  }

  @Test
  public void testInitializeDatastream() throws Exception {
    String sourceRegex = "\\w+Event";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream ds =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("testInitializeDatastream", _broker, sourceRegex, metadata);
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("testInitializeDatastream", getDefaultConfig(Optional.empty()));
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
        getDefaultConfig(Optional.empty()));
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
        new KafkaMirrorMakerConnector("testInitializeDatastreamWithBYOT", getDefaultConfig(Optional.empty()));
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
        new KafkaMirrorMakerConnector("testInitializeDatastreamWithBadSource", getDefaultConfig(Optional.empty()));
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  private KafkaTransportProviderAdmin getKafkaTransportProviderAdmin() {
    Properties props = new Properties();
    props.put("zookeeper.connect", _kafkaCluster.getZkConnection());
    props.put("bootstrap.servers", _kafkaCluster.getBrokers());
    return new KafkaTransportProviderAdmin(props);
  }

  @Test
  public void testPopulateDatastreamDestination() throws Exception {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()));
    Coordinator coordinator = createCoordinator(_kafkaCluster.getZkConnection(), "testPopulateDatastreamDestination");
    coordinator.addConnector("KafkaMirrorMaker", connector, new BroadcastStrategy(DEFAULT_MAX_TASKS), false,
        new SourceBasedDeduper(), null);
    String transportProviderName = "kafkaTransportProvider";
    KafkaTransportProviderAdmin transportProviderAdmin = getKafkaTransportProviderAdmin();
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
    Assert.assertEquals(transportProviderAdmin.getDestination(someTopic),
        String.format(stream.getDestination().getConnectionString(), someTopic));
  }

  @Test
  public void testFlushlessModeEnabled() throws Exception {
    Properties overrides = new Properties();
    overrides.put(KafkaMirrorMakerConnector.IS_FLUSHLESS_MODE_ENABLED, Boolean.TRUE.toString());
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.of(overrides)));
    Datastream ds = KafkaMirrorMakerConnectorTestUtils.createDatastream("testFlushlessModeEnabled", _broker, "Pizza",
        new StringMap());

    // assert that flushless task is created
    Assert.assertTrue(connector.createKafkaBasedConnectorTask(
        new DatastreamTaskImpl(Arrays.asList(ds))) instanceof FlushlessKafkaMirrorMakerConnectorTask);
  }

  @Test
  public void testFlushlessModeDisabled() throws Exception {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()));
    Datastream ds = KafkaMirrorMakerConnectorTestUtils.createDatastream("testFlushlessModeEnabled", _broker, "Pizza",
        new StringMap());

    // assert that flushless task is not created
    Assert.assertTrue(connector.createKafkaBasedConnectorTask(
        new DatastreamTaskImpl(Arrays.asList(ds))) instanceof KafkaMirrorMakerConnectorTask);
  }

  @Test
  public void testValidateDatastreamUpdatePausedPartitions() throws Exception {
    String topic = "testValidateDatastreamUpdatePausedPartitions";
    Map<String, Set<String>> pausedPartitions = new HashMap<>();
    Map<String, Set<String>> expectedPartitions = new HashMap<>();

    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()));
    Coordinator coordinator =
        createCoordinator(_kafkaCluster.getZkConnection(), "testValidateDatastreamUpdatePausedPartitions");
    coordinator.addConnector("KafkaMirrorMaker", connector, new BroadcastStrategy(DEFAULT_MAX_TASKS), false,
        new SourceBasedDeduper(), null);
    String transportProviderName = "kafkaTransportProvider";
    KafkaTransportProviderAdmin transportProviderAdmin = getKafkaTransportProviderAdmin();
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
    pausedPartitions.put(topic, new HashSet<>(Arrays.asList("1")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("1")));

    verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions);

    // Now add non-existent partition to list, and make sure it gets stripped off
    pausedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "99", "random", "1")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "1")));
    verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions);
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
    Datastream datastream =
        KafkaMirrorMakerConnectorTestUtils.createDatastream("pizzaStream", _broker, "\\w+Pizza");

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()));
    connector.start();

    // notify connector of new task
    connector.onAssignmentChange(Arrays.asList(task));

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
    connector.onAssignmentChange(Arrays.asList(task));

    if (!PollUtils.poll(() -> connector.process(DATASTREAM_STATE_QUERY + datastream.getName())
        .contains("\"manualPausedPartitions\":{\"SaltyPizza\":[\"0\"]}"), POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("manualPausedPartitions was not properly retrieved from process() after pause");
    }

    // manually resume partition for saltyTopic
    pausedPartitions.remove(saltyTopic);
    datastream.getMetadata()
        .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));

    // notify connector of resumed partition update
    connector.onAssignmentChange(Arrays.asList(task));

    if (!PollUtils.poll(() -> connector.process(DATASTREAM_STATE_QUERY + datastream.getName())
        .contains("\"manualPausedPartitions\":{}"), POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("manualPausedPartitions was not properly retrieved from process() after resume");
    }

    // simulate send error to auto-pause yummyTopic partition
    datastreamProducer.updateSendFailCondition((record) -> true);
    KafkaMirrorMakerConnectorTestUtils.produceEvents(yummyTopic, 1, _kafkaCluster);

    if (!PollUtils.poll(() -> connector.process(DATASTREAM_STATE_QUERY + datastream.getName())
        .contains("\"autoPausedPartitions\":{\"YummyPizza-0\":{\"reason\":\"SEND_ERROR\"}}"), POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("autoPausedPartitions was not properly retrieved from process() after auto-pause");
    }

    // update the send fail condition to allow the message to successfully flow through message is retried
    datastreamProducer.updateSendFailCondition((record) -> false);
    if (!PollUtils.poll(() -> connector.process(DATASTREAM_STATE_QUERY + datastream.getName())
        .contains("\"autoPausedPartitions\":{}"), POLL_PERIOD_MS, POLL_TIMEOUT_MS)) {
      Assert.fail("autoPausedPartitions was not properly retrieved from process() after auto-resume");
    }

    connector.stop();
  }

  @Test
  public void testProcessDatastreamStatesRequestInvalid() {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()));

    connector.start();

    Assert.assertNull(connector.process(""));
    Assert.assertNull(connector.process("/not_datastream_state?datastream=name"));
    Assert.assertNull(connector.process("/datastream_state?notdatastream=name"));

    connector.stop();

  }

  @Test
  public void testReduceDatastreamStates() {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()));

    String datastreamName = "testProcessDatastreamStates";

    // build instance 1 results
    Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions1 = new HashMap<>();
    Map<String, Set<String>> manualPausedPartitions1 = new HashMap<>();

    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";
    autoPausedPartitions1.put(new TopicPartition(yummyTopic, 0),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));
    autoPausedPartitions1.put(new TopicPartition(yummyTopic, 10),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));

    manualPausedPartitions1.put(saltyTopic, Sets.newHashSet("2", "5", "77"));
    manualPausedPartitions1.put(yummyTopic, Sets.newHashSet("4", "11", "23"));

    KafkaDatastreamStatesResponse process1Response =
        new KafkaDatastreamStatesResponse(datastreamName, autoPausedPartitions1, manualPausedPartitions1);

    // build instance 2 results
    Map<TopicPartition, PausedSourcePartitionMetadata> autoPausedPartitions2 = new HashMap<>();
    Map<String, Set<String>> manualPausedPartitions2 = new HashMap<>();

    autoPausedPartitions2.put(new TopicPartition(saltyTopic, 6),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));
    autoPausedPartitions2.put(new TopicPartition(saltyTopic, 17),
        new PausedSourcePartitionMetadata(() -> false, PausedSourcePartitionMetadata.Reason.SEND_ERROR));

    manualPausedPartitions2.put(saltyTopic, Sets.newHashSet("1", "9", "25"));
    manualPausedPartitions2.put(yummyTopic, Sets.newHashSet("19"));

    KafkaDatastreamStatesResponse process2Response =
        new KafkaDatastreamStatesResponse(datastreamName, autoPausedPartitions2, manualPausedPartitions2);

    Map<String, String> responseMap = new HashMap<>();
    responseMap.put("instance1", KafkaDatastreamStatesResponse.toJson(process1Response));
    responseMap.put("instance2", KafkaDatastreamStatesResponse.toJson(process2Response));

    String result = connector.reduce("/datastream_state?datastream=name", responseMap);

    Assert.assertEquals(result,
        "{\"instance2\":\"{\\\"datastream\\\":\\\"testProcessDatastreamStates\\\",\\\"autoPausedPartitions\\\""
            + ":{\\\"SaltyPizza-6\\\":{\\\"reason\\\":\\\"SEND_ERROR\\\"},\\\"SaltyPizza-17\\\":{\\\"reason\\\":"
            + "\\\"SEND_ERROR\\\"}},\\\"manualPausedPartitions\\\":{\\\"YummyPizza\\\":[\\\"19\\\"],\\\"SaltyPizza\\\":"
            + "[\\\"1\\\",\\\"9\\\",\\\"25\\\"]}}\",\"instance1\":\"{\\\"datastream\\\":\\\"testProcessDatastreamStates"
            + "\\\",\\\"autoPausedPartitions\\\":{\\\"YummyPizza-0\\\":{\\\"reason\\\":\\\"SEND_ERROR\\\"},\\\""
            + "YummyPizza-10\\\":{\\\"reason\\\":\\\"SEND_ERROR\\\"}},\\\"manualPausedPartitions\\\":{\\\"YummyPizza"
            + "\\\":[\\\"11\\\",\\\"23\\\",\\\"4\\\"],\\\"SaltyPizza\\\":[\\\"77\\\",\\\"2\\\",\\\"5\\\"]}}\"}");
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
