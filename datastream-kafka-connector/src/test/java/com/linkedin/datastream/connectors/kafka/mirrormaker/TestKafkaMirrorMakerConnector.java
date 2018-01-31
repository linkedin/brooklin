package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.util.Collections;
import java.util.Optional;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.connectors.kafka.AbstractKafkaConnector;
import com.linkedin.datastream.connectors.kafka.BaseKafkaZkTest;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import com.linkedin.datastream.server.CachedDatastreamReader;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.CoordinatorConfig;
import com.linkedin.datastream.server.DummyTransportProviderAdminFactory;
import com.linkedin.datastream.server.SourceBasedDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;

import static com.linkedin.datastream.server.assignment.BroadcastStrategyFactory.*;


@Test
public class TestKafkaMirrorMakerConnector extends BaseKafkaZkTest {

  private CachedDatastreamReader _cachedDatastreamReader;

  private Properties getDefaultConfig(Optional<Properties> override) {
    Properties config = new Properties();
    config.put(KafkaMirrorMakerConnector.CONFIG_DEFAULT_KEY_SERDE, "keySerde");
    config.put(KafkaMirrorMakerConnector.CONFIG_DEFAULT_VALUE_SERDE, "valueSerde");
    config.put(KafkaMirrorMakerConnector.CONFIG_COMMIT_INTERVAL_MILLIS, "10000");
    config.put(AbstractKafkaConnector.CONFIG_CONSUMER_FACTORY_CLASS, KafkaConsumerFactoryImpl.class.getName());
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
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithDestinationReuse() throws DatastreamValidationException {
    String sourceRegex = "\\w+Event";
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.TRUE.toString());
    Datastream ds = createDatastream("testInitializeDatastreamWithDestinationReuse", _broker, sourceRegex, metadata);
    KafkaMirrorMakerConnector connector =
        new KafkaMirrorMakerConnector("testInitializeDatastreamWithDestinationReuse", getDefaultConfig(Optional.empty()));
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
}
