/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.kafka.KafkaTransportProviderAdmin;
import com.linkedin.datastream.server.Coordinator;
import com.linkedin.datastream.server.SourceBasedDeduper;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;
import com.linkedin.datastream.server.assignment.BroadcastStrategy;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;
import com.linkedin.datastream.testutil.DatastreamTestUtils;


/**
 * Tests for {@link KafkaConnector}
 */
@Test
public class TestKafkaConnector extends BaseKafkaZkTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaConnector.class);

  static Properties getDefaultConfig(Properties override) {
    Properties config = new Properties();
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_KEY_SERDE, "keySerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_VALUE_SERDE, "valueSerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_COMMIT_INTERVAL_MILLIS, "10000");
    config.put(KafkaBasedConnectorConfig.CONFIG_CONSUMER_FACTORY_CLASS, KafkaConsumerFactoryImpl.class.getName());
    if (override != null) {
      config.putAll(override);
    }
    return config;
  }

  private Datastream createDatastream(String name, String topicName) {
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("kafka://" + _broker + "/" + topicName);
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString("whatever://bob");
    Datastream datastream = new Datastream();
    datastream.setName(name);
    datastream.setConnectorName("Kafka");
    datastream.setSource(source);
    datastream.setDestination(destination);
    datastream.setMetadata(new StringMap());
    return datastream;
  }

  @Test
  public void testConnectorWithStartPosition() throws UnsupportedEncodingException, DatastreamValidationException {
    String topicName = "testConnectorWithStartPosition";
    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName, 0, 100);
    long ts = System.currentTimeMillis();
    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName, 100, 100);
    Datastream ds = createDatastream("testConnectorPopulatesPartitions", topicName);
    Map<Integer, Long> offsets = Collections.singletonMap(0, 100L);
    KafkaConnector connector =
        new KafkaConnector("test", getDefaultConfig(null), "testCluster");
    ds.getMetadata().put(DatastreamMetadataConstants.START_POSITION, JsonUtils.toJson(offsets));
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testInitializeDatastreamWithNonexistentTopic() throws DatastreamValidationException {
    String topicName = "testInitializeDatastreamWithNonexistentTopic";
    Datastream ds = createDatastream("testInitializeDatastreamWithNonexistentTopic", topicName);
    KafkaConnector connector =
        new KafkaConnector("test", getDefaultConfig(null), "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test
  public void testPopulatingDefaultSerde() throws Exception {
    String topicName = "testPopulatingDefaultSerde";
    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName, 0, 100);
    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName, 100, 100);
    Datastream ds = createDatastream("testPopulatingDefaultSerde", topicName);
    KafkaConnector connector =
        new KafkaConnector("test", getDefaultConfig(null), "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());
    Assert.assertTrue(ds.getDestination().hasKeySerDe());
    Assert.assertEquals(ds.getDestination().getKeySerDe(), "keySerde");
    Assert.assertTrue(ds.getDestination().hasPayloadSerDe());
    Assert.assertEquals(ds.getDestination().getPayloadSerDe(), "valueSerde");
  }

  @Test
  public void testConnectorPopulatesPartitions() throws UnsupportedEncodingException, DatastreamValidationException {
    String topicName = "testConnectorPopulatesPartitions";
    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName, 0, 10);

    Datastream ds = createDatastream("testConnectorPopulatesPartitions", topicName);
    KafkaConnector connector =
        new KafkaConnector("test", getDefaultConfig(null), "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());
    Assert.assertEquals(ds.getSource().getPartitions().intValue(), 1);
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testConnectorValidatesWhitelistedBroker() throws DatastreamValidationException {
    String topicName = "testConnectorValidatesWhitelistedBroker";

    Datastream ds = createDatastream("testConnectorPopulatesPartitions", topicName);
    Properties override = new Properties();
    override.put(KafkaConnector.CONFIG_WHITE_LISTED_CLUSTERS, "randomBroker:2546");
    KafkaConnector connector = new KafkaConnector("test", getDefaultConfig(override), "testCluster");
    connector.initializeDatastream(ds, Collections.emptyList());
  }

  @Test
  public void testGroupIdAssignment() throws Exception {
    executeTestGroupIdAssignment(false);
  }

  @Test
  public void testGroupIdAssignmentWithHashing() throws Exception {
    executeTestGroupIdAssignment(true);
  }

  private void executeTestGroupIdAssignment(boolean isGroupIdHashingEnabled) throws Exception {

    String clusterName = "testGroupIdAssignment";
    KafkaGroupIdConstructor groupIdConstructor =
        new KafkaGroupIdConstructor(isGroupIdHashingEnabled, "testGroupIdAssignment");

    String topicName1 = "topic1";
    String topicName2 = "topic2";
    String topicName3 = "topic3";

    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName1, 0, 100);
    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName2, 0, 100);
    TestKafkaConnectorTask.produceEvents(_kafkaCluster, _zkUtils, topicName3, 0, 100);

    Properties properties = getDefaultConfig(null);
    properties.put(AbstractKafkaConnector.IS_GROUP_ID_HASHING_ENABLED, Boolean.toString(isGroupIdHashingEnabled));
    KafkaConnector connector = new KafkaConnector("MirrorMakerConnector", properties, clusterName);

    Coordinator coordinator = TestKafkaConnectorUtils.createCoordinator(_kafkaCluster.getZkConnection(), clusterName);
    coordinator.addConnector("Kafka", connector, new BroadcastStrategy(Optional.empty()), false,
        new SourceBasedDeduper(), null);
    String transportProviderName = "kafkaTransportProvider";
    KafkaTransportProviderAdmin transportProviderAdmin =
        TestKafkaConnectorUtils.createKafkaTransportProviderAdmin(_kafkaCluster);
    coordinator.addTransportProvider(transportProviderName, transportProviderAdmin);
    coordinator.start();

    // create datastream without any group ID override - group ID should get constructed
    Datastream datastream1 = createDatastream("datastream1", topicName1);
    datastream1.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream1);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream1);
    LOG.info("datastream1 groupID: {}", datastream1.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream1 task prefix:: {}", datastream1.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    // create datastream without any group ID override, but same topic as datastream1
    // - datastream1's group ID should get copied
    Datastream datastream2 = createDatastream("datastream2", topicName1);
    datastream2.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream2);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream2);
    LOG.info("datastream2 groupID: {}", datastream2.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream2 task prefix:: {}", datastream2.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    // create datastream with group ID override and same source as datastream 1 & 2
    // - group ID override should take precedence
    Datastream datastream3 = createDatastream("datastream3", topicName1);
    datastream3.getMetadata().put(DatastreamMetadataConstants.GROUP_ID, "datastream3");
    datastream3.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream3);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream3);
    LOG.info("datastream3 groupID: {}", datastream3.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream3 task prefix:: {}", datastream3.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    // create a datastream with different source than datastream1/2/3 and overridden group ID
    // - overridden group ID should be used.
    Datastream datastream4 = createDatastream("datastream4", topicName2);
    datastream4.getMetadata().put(DatastreamMetadataConstants.GROUP_ID, "randomId");
    datastream4.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream4);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream4);
    LOG.info("datastream4 groupID: {}", datastream4.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream4 task prefix:: {}", datastream4.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    // create a datastream with same source as datastream4 - it should use same overridden group ID from datastream4
    Datastream datastream5 = createDatastream("datastream5", topicName2);
    datastream5.getMetadata().put(DatastreamMetadataConstants.GROUP_ID, "randomId");
    datastream5.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream5);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream5);
    LOG.info("datastream5 groupID: {}", datastream5.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream5  task prefix:: {}", datastream5.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    // create datastream with a different source than any of the above datastreams - group ID should be constructed
    Datastream datastream6 = createDatastream("datastream6", topicName3);
    datastream6.setTransportProviderName(transportProviderName);
    coordinator.initializeDatastream(datastream6);
    DatastreamTestUtils.storeDatastreams(_zkClient, clusterName, datastream6);
    LOG.info("datastream6 groupID: {}", datastream6.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    LOG.info("datastream6 task prefix: {}", datastream5.getMetadata().get(DatastreamMetadataConstants.TASK_PREFIX));

    Assert.assertEquals(datastream1.getMetadata().get(DatastreamMetadataConstants.GROUP_ID),
        groupIdConstructor.constructGroupId(datastream1));
    Assert.assertEquals(datastream2.getMetadata().get(DatastreamMetadataConstants.GROUP_ID),
        datastream1.getMetadata().get(DatastreamMetadataConstants.GROUP_ID));
    Assert.assertEquals(datastream3.getMetadata().get(DatastreamMetadataConstants.GROUP_ID), "datastream3");

    Assert.assertEquals(datastream4.getMetadata().get(DatastreamMetadataConstants.GROUP_ID), "randomId");
    Assert.assertEquals(datastream5.getMetadata().get(DatastreamMetadataConstants.GROUP_ID), "randomId");

    Assert.assertEquals(datastream6.getMetadata().get(DatastreamMetadataConstants.GROUP_ID),
        groupIdConstructor.constructGroupId(datastream6));

    coordinator.stop();
  }
}
