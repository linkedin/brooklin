/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.kafka.KafkaDatastreamMetadataConstants;
import com.linkedin.datastream.kafka.factory.KafkaConsumerFactory;
import com.linkedin.datastream.kafka.factory.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.zk.ZkAdapter;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;
import com.linkedin.datastream.testutil.DatastreamEmbeddedZookeeperKafkaCluster;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anySetOf;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;


/**
 * Tests for {@link KafkaConnectorTask}
 */
public class TestKafkaConnectorTask extends BaseKafkaZkTest {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaConnectorTask.class);
  private static final int POLL_TIMEOUT_MS = 25000;
  private static final long CONNECTOR_AWAIT_STOP_TIMEOUT_MS = 30000;

  protected static void produceEvents(DatastreamEmbeddedZookeeperKafkaCluster cluster, AdminClient adminClient,
      String topic, int index, int numEvents) throws UnsupportedEncodingException {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.getBrokers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 100);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

    createTopic(adminClient, topic);
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < numEvents; i++) {
        final int finalIndex = index;
        producer.send(
            new ProducerRecord<>(topic, ("key-" + index).getBytes(StandardCharsets.UTF_8), ("value-" + index).getBytes(
                StandardCharsets.UTF_8)),
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
        new KafkaBasedConnectorTaskMetrics(TestKafkaConnectorTask.class.getName(), "testConsumer", LOG, true);
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
  public void testConsumeWithStartingOffsetAndNoResetStrategy() throws Exception {
    String topic = "pizza1";
    createTopic(_adminClient, topic);

    LOG.info("Sending first set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _adminClient, topic, 0, 100);
    Map<Integer, Long> startOffsets = Collections.singletonMap(0, 100L);

    LOG.info("Sending second set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _adminClient, topic, 100, 100);

    //start
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    Datastream datastream = getDatastream(_broker, topic);

    // Unable to set the start position, OffsetToTimestamp is returning null in the embedded Kafka cluster.
    datastream.getMetadata().put(DatastreamMetadataConstants.START_POSITION, JsonUtils.toJson(startOffsets));
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTaskWithAutoOffsetResetConfig(task, "earliest");

    // validate auto.offset.reset config is overridden to none (given the start offsets)
    Assert.assertEquals(connectorTask.getConsumerAutoOffsetResetConfig(), "none");


    LOG.info("Sending third set of events");

    //send 100 more msgs
    produceEvents(_kafkaCluster, _adminClient, topic, 1000, 100);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 200, 100, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer 200 msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testConsumeWithStartingOffsetAndResetStrategy() throws Exception {
    String topic = "pizza1";
    createTopic(_adminClient, topic);

    LOG.info("Sending first set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _adminClient, topic, 0, 100);
    Map<Integer, Long> startOffsets = Collections.singletonMap(0, 100L);

    LOG.info("Sending second set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _adminClient, topic, 100, 100);

    //start
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    Datastream datastream = getDatastream(_broker, topic);
    // set system.start.position in the metadata
    datastream.getMetadata().put(DatastreamMetadataConstants.START_POSITION, JsonUtils.toJson(startOffsets));
    // set system.auto.offset.reset strategy in metadata to ensure it is ignored in presence of start offsets
    datastream.getMetadata().put(KafkaDatastreamMetadataConstants.CONSUMER_OFFSET_RESET_STRATEGY, "earliest");
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTaskWithAutoOffsetResetConfig(task, "earliest");

    // validate auto.offset.reset config is overridden to none (given the start offsets)
    Assert.assertEquals(connectorTask.getConsumerAutoOffsetResetConfig(), "none");


    LOG.info("Sending third set of events");

    //send 100 more msgs
    produceEvents(_kafkaCluster, _adminClient, topic, 1000, 100);

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
    createTopic(_adminClient, topic);

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
          doAnswer(invocation -> {
            remainingCommitSyncCalls.countDown();
            return null;
          }).when(result).commitSync(any(Duration.class));
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
    createTopic(_adminClient, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _adminClient, topic, 0, 1);

    LOG.info("Creating and Starting KafkaConnectorTask");
    Datastream datastream = getDatastream(_broker, topic);
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTask(task);

    LOG.info("Producing 100 msgs to topic: " + topic);
    produceEvents(_kafkaCluster, _adminClient, topic, 1000, 100);

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
    createTopic(_adminClient, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _adminClient, topic, 0, 1);

    LOG.info("Creating and Starting KafkaConnectorTask");
    final Datastream datastream = getDatastream(_broker, topic);
    final DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    final MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    final KafkaConnectorTask connectorTask = createKafkaConnectorTask(task, config);

    LOG.info("Producing 100 msgs to topic: " + topic);
    produceEvents(_kafkaCluster, _adminClient, topic, 1000, 100);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 100, 100, POLL_TIMEOUT_MS)) {
      Assert.fail("did not transfer 100 msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(CONNECTOR_AWAIT_STOP_TIMEOUT_MS, TimeUnit.MILLISECONDS),
        "did not shut down on time");
  }

  @Test
  public void testRewindWhenSkippingMessage() throws Exception {
    String topic = "pizza1";
    createTopic(_adminClient, topic);
    AtomicInteger i = new AtomicInteger(0);

    //Throw exactly one error message when sending the messages, causing partition to be paused for exactly once
    MockDatastreamEventProducer datastreamProducer =
        new MockDatastreamEventProducer(r -> (i.addAndGet(1) == 1));
    Datastream datastream = getDatastream(_broker, topic);

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    TopicPartition topicPartition = new TopicPartition("pizza1", 0);
    KafkaConnectorTask connectorTask = spy(new KafkaConnectorTask(new KafkaBasedConnectorConfigBuilder()
        .setPausePartitionOnError(true).setPauseErrorPartitionDuration(Duration.ofDays(1)).build(), task, "",
        new KafkaGroupIdConstructor(false, "testCluster")));

    Map<TopicPartition, List<ConsumerRecord<Object, Object>>> records =  new HashMap<>();
    records.put(topicPartition, ImmutableList.of(new ConsumerRecord<>("pizza1", 0, 0, new Object(), new Object()),
        new ConsumerRecord<>("pizza1", 0, 0, new Object(), new Object())));

    ConsumerRecords<?, ?> consumerRecords = new ConsumerRecords<>(records);

    doReturn(consumerRecords).when(connectorTask).pollRecords(anyLong());
    doAnswer(a -> null).when(connectorTask).seekToLastCheckpoint(anySetOf(TopicPartition.class));
    Thread t = new Thread(connectorTask, "connector thread");
    t.setDaemon(true);
    t.setUncaughtExceptionHandler((t1, e) -> Assert.fail("connector thread died", e));
    t.start();
    if (!connectorTask.awaitStart(60, TimeUnit.SECONDS)) {
      Assert.fail("connector did not start within timeout");
    }

    //Wait a small period of time that some messages will be skipped
    Thread.sleep(1000);
    verify(connectorTask, times(1)).rewindAndPausePartitionOnException(eq(topicPartition),
        any(Exception.class));
    //Verify that we have call at least seekToLastCheckpoint twice as the skip messages also trigger this
    verify(connectorTask, atLeast(2)).seekToLastCheckpoint(ImmutableSet.of(topicPartition));
    connectorTask.stop();
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
    createTopic(_adminClient, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _adminClient, topic, 0, 1);

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
      return null;
    }).when(datastreamProducer).send(any(), any());

    LOG.info("Creating and Starting KafkaConnectorTask");
    Datastream datastream = getDatastream(_broker, topic);
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask = createKafkaConnectorTask(task);

    LOG.info("Producing 100 msgs to topic: " + topic);
    produceEvents(_kafkaCluster, _adminClient, topic, 1000, 100);

    if (!PollUtils.poll(() -> state.messagesProcessed >= 100, 100, POLL_TIMEOUT_MS)) {
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
    createTopic(_adminClient, topic);

    LOG.info("Sending first event, to avoid an empty topic.");
    produceEvents(_kafkaCluster, _adminClient, topic, 0, 1);

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

  private KafkaConnectorTask createKafkaConnectorTaskWithAutoOffsetResetConfig(DatastreamTaskImpl task,
      String autoOffsetResetStrategy) throws InterruptedException {
    Properties consumerProperties = new Properties();
    consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetResetStrategy);
    return createKafkaConnectorTask(task, new KafkaBasedConnectorConfigBuilder()
        .setConsumerProps(consumerProperties).build());
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
