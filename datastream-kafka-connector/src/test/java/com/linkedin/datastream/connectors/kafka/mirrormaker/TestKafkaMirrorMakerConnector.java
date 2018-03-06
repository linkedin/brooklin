package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import kafka.admin.AdminUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.DatastreamUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.kafka.BaseKafkaZkTest;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
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
  private static final int POLL_TIMEOUT_MS = 25000;

  private CachedDatastreamReader _cachedDatastreamReader;

  private Properties getDefaultConfig(Optional<Properties> override) {
    Properties config = new Properties();
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_KEY_SERDE, "keySerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_VALUE_SERDE, "valueSerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_COMMIT_INTERVAL_MILLIS, "10000");
    config.put(KafkaBasedConnectorConfig.CONFIG_CONSUMER_FACTORY_CLASS, KafkaConsumerFactoryImpl.class.getName());
    override.ifPresent(o -> config.putAll(o));
    return config;
  }

  protected static Datastream createDatastream(String name, String broker, String sourceRegex, StringMap metadata) {
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("kafka://" + broker + "/" + sourceRegex);
    Datastream datastream = new Datastream();
    datastream.setName(name);
    datastream.setConnectorName("KafkaMirrorMaker");
    datastream.setSource(source);
    datastream.setMetadata(metadata);
    datastream.setTransportProviderName("transportProvider");
    return datastream;
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
    Datastream ds = createDatastream("testInitializeDatastream", _broker, sourceRegex, metadata);
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("testInitializeDatastream", getDefaultConfig(Optional.empty()));
    connector.initializeDatastream(ds, Collections.emptyList());

    sourceRegex = "SpecificTopic";
    ds = createDatastream("testInitializeDatastream2", _broker, sourceRegex, metadata);
    connector.initializeDatastream(ds, Collections.emptyList());

    sourceRegex = "(\\w+Event)|^(Topic)";
    ds = createDatastream("testInitializeDatastream3", _broker, sourceRegex, metadata);
    connector.initializeDatastream(ds, Collections.emptyList());

    sourceRegex = "^(?!__)\\w+";
    ds = createDatastream("testInitializeDatastream4", _broker, sourceRegex, metadata);
    connector.initializeDatastream(ds, Collections.emptyList());

    Assert.assertTrue(DatastreamUtils.isConnectorManagedDestination(ds));
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithDestinationReuse() throws DatastreamValidationException {
    String sourceRegex = "\\w+Event";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.TRUE.toString());
    Datastream ds = createDatastream("testInitializeDatastreamWithDestinationReuse", _broker, sourceRegex, metadata);
    KafkaMirrorMakerConnector connector = new KafkaMirrorMakerConnector("testInitializeDatastreamWithDestinationReuse",
        getDefaultConfig(Optional.empty()));
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithBYOT() throws DatastreamValidationException {
    String sourceRegex = "\\w+Event";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.IS_USER_MANAGED_DESTINATION_KEY, Boolean.TRUE.toString());
    Datastream ds = createDatastream("testInitializeDatastreamWithBYOT", _broker, sourceRegex, metadata);
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("testInitializeDatastreamWithBYOT", getDefaultConfig(Optional.empty()));
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithBadSource() throws DatastreamValidationException {
    String sourceRegex = "*Event*";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream ds = createDatastream("testInitializeDatastreamWithBadSource", _broker, sourceRegex, metadata);
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
    Datastream stream = createDatastream("testPopulateDatastreamDestination", _broker, "\\w+Event", metadata);
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
    Datastream ds = createDatastream("testFlushlessModeEnabled", _broker, "Pizza", new StringMap());

    // assert that flushless task is created
    Assert.assertTrue(connector.createKafkaBasedConnectorTask(
        new DatastreamTaskImpl(Arrays.asList(ds))) instanceof FlushlessKafkaMirrorMakerConnectorTask);
  }

  @Test
  public void testFlushlessModeDisabled() throws Exception {
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("MirrorMakerConnector", getDefaultConfig(Optional.empty()));
    Datastream ds = createDatastream("testFlushlessModeEnabled", _broker, "Pizza", new StringMap());

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
    Coordinator coordinator = createCoordinator(_kafkaCluster.getZkConnection(), "testPopulateDatastreamDestination");
    coordinator.addConnector("KafkaMirrorMaker", connector, new BroadcastStrategy(DEFAULT_MAX_TASKS), false,
        new SourceBasedDeduper(), null);
    String transportProviderName = "kafkaTransportProvider";
    KafkaTransportProviderAdmin transportProviderAdmin = getKafkaTransportProviderAdmin();
    coordinator.addTransportProvider(transportProviderName, transportProviderAdmin);
    coordinator.start();

    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream datastream = createDatastream("testPopulateDatastreamDestination", _broker, topic, metadata);
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
    if (!PollUtils.poll(() -> verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions), 100,
        POLL_TIMEOUT_MS)) {
      Assert.fail("verifyPausedPartitions failed");
    }

    // Make sure multiple *s and numbers is converted to a list of unique partitions
    pausedPartitions.put(topic, new HashSet<>(Arrays.asList("*", "2", "1", "*")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "1")));
    if (!PollUtils.poll(() -> verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions), 100,
        POLL_TIMEOUT_MS)) {
      Assert.fail("verifyPausedPartitions failed");
    }

    // Make sure numbers aren't touched
    pausedPartitions.put(topic, new HashSet<>(Arrays.asList("1")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("1")));
    if (!PollUtils.poll(() -> verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions), 100,
        POLL_TIMEOUT_MS)) {
      Assert.fail("verifyPausedPartitions failed");
    }

    // Now add non-existent partition to list, and make sure it gets stripped off
    pausedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "99", "random", "1")));
    // prepare expected partitions for validation
    expectedPartitions.put(topic, new HashSet<>(Arrays.asList("0", "1")));
    if (!PollUtils.poll(() -> verifyPausedPartitions(connector, datastream, pausedPartitions, expectedPartitions), 100,
        POLL_TIMEOUT_MS)) {
      Assert.fail("verifyPausedPartitions failed");
    }
  }

  private boolean verifyPausedPartitions(Connector connector, Datastream datastream,
      Map<String, Set<String>> pausedPartitions, Map<String, Set<String>> expectedPartitions) {
    try {
      datastream.getMetadata()
          .put(DatastreamMetadataConstants.PAUSED_SOURCE_PARTITIONS_KEY, JsonUtils.toJson(pausedPartitions));
      connector.validateUpdateDatastreams(Collections.singletonList(datastream), Collections.singletonList(datastream));
      Assert.assertEquals(expectedPartitions, DatastreamUtils.getDatastreamSourcePartitions(datastream));
    } catch (Exception e) {
      LOG.warn("verifyPausedPartitions failed with error: " + e);
      return false;
    }
    return true;
  }
}
