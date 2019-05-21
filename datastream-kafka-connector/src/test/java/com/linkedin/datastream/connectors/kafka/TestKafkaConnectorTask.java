/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import kafka.utils.ZkUtils;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.diag.KafkaPositionValue;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.DatastreamEmbeddedZookeeperKafkaCluster;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;


/**
 * Tests for {@link KafkaConnectorTask}
 */
public class TestKafkaConnectorTask extends BaseKafkaZkTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaConnectorTask.class);
  private static final int POLL_TIMEOUT_MS = 25000;
  private static final long CONNECTOR_AWAIT_STOP_TIMEOUT_MS = 30000;

  protected static void produceEvents(DatastreamEmbeddedZookeeperKafkaCluster cluster, ZkUtils zkUtils, String topic, int index, int numEvents)
      throws UnsupportedEncodingException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBrokers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 100);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

    createTopic(zkUtils, topic);
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < numEvents; i++) {
        final int finalIndex = index;
        producer.send(
            new ProducerRecord<>(topic, ("key-" + index).getBytes("UTF-8"), ("value-" + index).getBytes("UTF-8")),
            (metadata, exception) -> {
              if (exception == null) {
                LOG.info("send completed for event {} at offset {}", finalIndex, metadata.offset());
              } else {
                throw new RuntimeException("Failed to send message.", exception);
              }
            });
        index++;
      }
      producer.flush();
    }
  }

  @Test
  public void testKafkaGroupId() throws Exception {
    KafkaGroupIdConstructor groupIdConstructor = new KafkaGroupIdConstructor(false, "testCluster");
    String topic = "MyTopicForGrpId";
    Datastream datastream1 = getDatastream(_broker, topic);
    Datastream datastream2 = getDatastream(_broker, topic);

    DatastreamTaskImpl task = new DatastreamTaskImpl(Arrays.asList(datastream1, datastream2));
    KafkaBasedConnectorTaskMetrics consumerMetrics =
        new KafkaBasedConnectorTaskMetrics(TestKafkaConnectorTask.class.getName(), "testConsumer", LOG);
    consumerMetrics.createEventProcessingMetrics();

    String defaultGrpId =
        datastream1.getSource().getConnectionString() + "-to-" + datastream1.getDestination().getConnectionString();

    // Testing with default group id
    Assert.assertEquals(KafkaConnectorTask.getKafkaGroupId(task, groupIdConstructor, consumerMetrics, LOG), defaultGrpId);

    // Test with setting explicit group id in one datastream
    datastream1.getMetadata().put(ConsumerConfig.GROUP_ID_CONFIG, "MyGroupId");
    Assert.assertEquals(KafkaConnectorTask.getKafkaGroupId(task, groupIdConstructor, consumerMetrics, LOG), "MyGroupId");

    // Test with explicitly setting group id in both datastream
    datastream2.getMetadata().put(ConsumerConfig.GROUP_ID_CONFIG, "MyGroupId");
    Assert.assertEquals(KafkaConnectorTask.getKafkaGroupId(task, groupIdConstructor, consumerMetrics, LOG), "MyGroupId");

    // now set different group ids in 2 datastreams and make sure validation fails
    datastream2.getMetadata().put(ConsumerConfig.GROUP_ID_CONFIG, "invalidGroupId");
    boolean exceptionSeen = false;
    try {
      KafkaConnectorTask.getKafkaGroupId(task, groupIdConstructor, consumerMetrics, LOG);
    } catch (DatastreamRuntimeException e) {
      exceptionSeen = true;
    }
    Assert.assertTrue(exceptionSeen);
  }

  @Test
  public void testConsumeWithStartingOffset() throws Exception {
    String topic = "pizza1";
    createTopic(_zkUtils, topic);

    LOG.info("Sending first set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _zkUtils, topic, 0, 100);
    Map<Integer, Long> startOffsets = Collections.singletonMap(0, 100L);

    LOG.info("Sending second set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _zkUtils, topic, 100, 100);

    //start
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    Datastream datastream = getDatastream(_broker, topic);

    // Unable to set the start position, OffsetToTimestamp is returning null in the embedded Kafka cluster.
    datastream.getMetadata().put(DatastreamMetadataConstants.START_POSITION, JsonUtils.toJson(startOffsets));
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTask(task);

    LOG.info("Sending third set of events");

    //send 100 more msgs
    produceEvents(_kafkaCluster, _zkUtils, topic, 1000, 100);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 200, 100, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer 200 msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testCommittingOffsetRegularly() throws Exception {
    String topic = "pizza1";
    createTopic(_zkUtils, topic);

    //start
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    Datastream datastream = getDatastream(_broker, topic);

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    // Set up a factory to create a Kafka consumer that tracks how many times commitSync is invoked
    CountDownLatch remainingCommitSyncCalls = new CountDownLatch(3);
    KafkaConsumerFactory<byte[], byte[]> kafkaConsumerFactory = new KafkaConsumerFactoryImpl() {
        @Override
        public Consumer<byte[], byte[]> createConsumer(Properties properties) {
          Consumer<byte[], byte[]> result = spy(super.createConsumer(properties));
          doAnswer(invocation -> { remainingCommitSyncCalls.countDown(); return null; })
              .when(result).commitSync(any(Duration.class));
          return result;
        }
      };

    KafkaConnectorTask connectorTask = createKafkaConnectorTask(task, new KafkaBasedConnectorConfigBuilder()
        .setConsumerFactory(kafkaConsumerFactory).build());

    // Wait for KafkaConnectorTask to invoke commitSync on Kafka consumer
    Assert.assertTrue(remainingCommitSyncCalls.await(10, TimeUnit.SECONDS),
        "Kafka consumer commitSync was not invoked as often as expected");

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testConsumerBaseCase() throws Exception {
    String topic = "Pizza2";
    createTopic(_zkUtils, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _zkUtils, topic, 0, 1);

    LOG.info("Creating and Starting KafkaConnectorTask");
    Datastream datastream = getDatastream(_broker, topic);
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTask(task);

    LOG.info("Producing 100 msgs to topic: " + topic);
    produceEvents(_kafkaCluster, _zkUtils, topic, 1000, 100);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 100, 100, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer 100 msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testConsumerPositionTracking() throws Exception {
    final KafkaBasedConnectorConfig config = new KafkaBasedConnectorConfigBuilder().build();

    final String topic = "ChicagoStylePizza";
    createTopic(_zkUtils, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _zkUtils, topic, 0, 1);

    LOG.info("Creating and Starting KafkaConnectorTask");
    final Datastream datastream = getDatastream(_broker, topic);
    final DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    final MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    final KafkaConnectorTask connectorTask = createKafkaConnectorTask(task, config);

    LOG.info("Producing 100 msgs to topic: " + topic);
    produceEvents(_kafkaCluster, _zkUtils, topic, 1000, 100);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 100, 100, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer 100 msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    // Update Kafka connector's position data
    final Optional<KafkaPositionTracker> kafkaPositionTracker = connectorTask.getKafkaPositionTracker();
    Assert.assertTrue(kafkaPositionTracker.isPresent());
    try (final Consumer<?, ?> consumer = kafkaPositionTracker.get().getConsumerSupplier().get()) {
      kafkaPositionTracker.get().queryBrokerForLatestOffsets(consumer, Collections.singleton(new TopicPartition(topic, 0)));
    }

    // Test position data
    final Optional<KafkaPositionValue> position = kafkaPositionTracker.get()
        .getPositions()
        .entrySet()
        .stream()
        .filter(e -> e.getKey().getTopic().equals(topic))
        .filter(e -> e.getKey().getPartition() == 0)
        .map(Map.Entry::getValue)
        .findAny();
    Assert.assertTrue(position.isPresent());
    Assert.assertEquals(position.get().getBrokerOffset(), position.get().getConsumerOffset());
    final DatastreamProducerRecord lastEvent = datastreamProducer.getEvents()
        .get(datastreamProducer.getEvents().size() - 1);
    Assert.assertNotNull(position.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(lastEvent.getEventsSourceTimestamp(),
        position.get().getLastRecordReceivedTimestamp().toEpochMilli());

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testConsumerProperties() {
    Properties overrides = new Properties();
    String groupId = "groupId";
    KafkaConnectionString connectionString = KafkaConnectionString.valueOf("kafka://MyBroker:10251/MyTopic");
    Properties actual = KafkaConnectorTask.getKafkaConsumerProperties(overrides, groupId, connectionString);

    Properties expected = new Properties();
    expected.put("auto.offset.reset", "none");
    expected.put("bootstrap.servers", "MyBroker:10251");
    expected.put("enable.auto.commit", "false");
    expected.put("group.id", "groupId");
    expected.put("security.protocol", "PLAINTEXT");

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testSslConsumerProperties() {
    Properties overrides = new Properties();
    String groupId = "groupId";
    KafkaConnectionString connectionString = KafkaConnectionString.valueOf("kafkassl://MyBroker:10251/MyTopic");
    Properties actual = KafkaConnectorTask.getKafkaConsumerProperties(overrides, groupId, connectionString);

    Properties expected = new Properties();
    expected.put("auto.offset.reset", "none");
    expected.put("bootstrap.servers", "MyBroker:10251");
    expected.put("enable.auto.commit", "false");
    expected.put("group.id", "groupId");
    expected.put("security.protocol", "SSL");

    Assert.assertEquals(actual, expected);
  }

  @Test
  public void testFlakyProducer() throws Exception {
    String topic = "pizza3";
    createTopic(_zkUtils, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _zkUtils, topic, 0, 1);

    class State {
      int messagesProcessed = 0;
      int pendingErrors = 3;
    }
    State state = new State();

    DatastreamEventProducer datastreamProducer = Mockito.mock(DatastreamEventProducer.class);
    doAnswer(invocation -> {
      if (state.pendingErrors > 0) {
        state.pendingErrors--;
        throw new RuntimeException("Flaky Exception");
      }
      state.messagesProcessed++;
      state.pendingErrors = 3;
      return null;
    }).when(datastreamProducer).send(any(), any());

    LOG.info("Creating and Starting KafkaConnectorTask");
    Datastream datastream = getDatastream(_broker, topic);
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTask(task);

    LOG.info("Producing 100 msgs to topic: " + topic);
    produceEvents(_kafkaCluster, _zkUtils, topic, 1000, 100);

    if (!PollUtils.poll(() -> state.messagesProcessed == 100, 100, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer 100 msgs within timeout. transferred " + state.messagesProcessed);
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testFlakyConsumer() throws Exception {
    String topic = "Pizza2";
    createTopic(_zkUtils, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _zkUtils, topic, 0, 1);

    LOG.info("Creating and Starting KafkaConnectorTask");
    Datastream datastream = getDatastream(_broker, topic);
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setZkAdapter(Mockito.mock(ZkAdapter.class));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTask(task);

    KafkaConnectorTask spiedConnectorTask = Mockito.spy(connectorTask);
    KafkaConsumer mockKafkaConsumer = Mockito.mock(KafkaConsumer.class);
    doReturn(mockKafkaConsumer).when(spiedConnectorTask).createKafkaConsumer(Mockito.any());
    boolean exceptionThrown = false;
    try {
      spiedConnectorTask.run();
    } catch (DatastreamRuntimeException ex) {
      exceptionThrown = true;
    }
    Assert.assertTrue(exceptionThrown);
    verify(spiedConnectorTask, Mockito.atLeast(5)).handlePollRecordsException(any());
  }

  static Datastream getDatastream(String broker, String topic) {
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("kafka://" + broker + "/" + topic);
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString("whatever://bob_" + topic);
    Datastream datastream = new Datastream();
    datastream.setName("datastream_for_" + topic);
    datastream.setConnectorName("whatever");
    datastream.setSource(source);
    datastream.setDestination(destination);
    datastream.setTransportProviderName("default");
    datastream.setMetadata(new StringMap());
    datastream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(datastream));
    return datastream;
  }

  private KafkaConnectorTask createKafkaConnectorTask(DatastreamTaskImpl task) throws InterruptedException {
    return createKafkaConnectorTask(task, new KafkaBasedConnectorConfigBuilder().build());
  }

  private KafkaConnectorTask createKafkaConnectorTask(DatastreamTaskImpl task, KafkaBasedConnectorConfig connectorConfig)
      throws InterruptedException {

    KafkaConnectorTask connectorTask = new KafkaConnectorTask(connectorConfig, task, "",
        new KafkaGroupIdConstructor(false, "testCluster"));

    Thread t = new Thread(connectorTask, "connector thread");
    t.setDaemon(true);
    t.setUncaughtExceptionHandler((t1, e) -> Assert.fail("connector thread died", e));
    t.start();
    if (!connectorTask.awaitStart(60, TimeUnit.SECONDS)) {
      Assert.fail("connector did not start within timeout");
    }
    return connectorTask;
  }
}
