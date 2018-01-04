package com.linkedin.datastream.connectors.kafka;

import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.kafka.EmbeddedZookeeperKafkaCluster;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamTaskImpl;


public class TestKafkaConnectorTask {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaConnectorTask.class);
  private static final int POLL_TIMEOUT_MS = 25000;

  private EmbeddedZookeeperKafkaCluster _kafkaCluster;
  private ZkUtils _zkUtils;
  private String _broker;

  @BeforeTest
  public void setup() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry(), "TestKafkaConnectorTask");
    Properties kafkaConfig = new Properties();
    // we will disable auto topic creation for this test file
    kafkaConfig.setProperty("auto.create.topics.enable", Boolean.FALSE.toString());
    _kafkaCluster = new EmbeddedZookeeperKafkaCluster(kafkaConfig);
    _kafkaCluster.startup();
    _zkUtils =
        new ZkUtils(new ZkClient(_kafkaCluster.getZkConnection()), new ZkConnection(_kafkaCluster.getZkConnection()),
            false);
    _broker = _kafkaCluster.getBrokers().split("\\s*,\\s*")[0];
  }

  @AfterTest
  public void teardown() throws Exception {
    _zkUtils.close();
    _kafkaCluster.shutdown();
  }

  public static void produceEvents(EmbeddedZookeeperKafkaCluster cluster, ZkUtils zkUtils, String topic, int index, int numEvents)
      throws UnsupportedEncodingException {
    Properties props = new Properties();
    props.put("bootstrap.servers", cluster.getBrokers());
    props.put("acks", "all");
    props.put("retries", 100);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
    props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());

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

  private static void createTopic(ZkUtils zkUtils, String topic) {
    if (!AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, 1, 2, new Properties(), null);
    }
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

    // Unable to set the start position, OffsetToTimestamp is returning null in the embedded kafka cluster.
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
    Assert.assertTrue(connectorTask.awaitStop(5000, TimeUnit.MILLISECONDS), "did not shut down on time");
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
    Assert.assertTrue(connectorTask.awaitStop(5000, TimeUnit.MILLISECONDS), "did not shut down on time");
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
    Assert.assertTrue(connectorTask.awaitStop(5000, TimeUnit.MILLISECONDS), "did not shut down on time");
  }

  private Datastream getDatastream(String broker, String topic) {
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
    KafkaConnectorTask connectorTask =
        new KafkaConnectorTask(new KafkaConsumerFactoryImpl(), new Properties(), task, 1000, Duration.ofSeconds(0), 5);
    Thread t = new Thread(connectorTask, "connector thread");
    t.setDaemon(true);
    t.setUncaughtExceptionHandler((t1, e) -> {
      Assert.fail("connector thread died", e);
    });
    t.start();
    if (!connectorTask.awaitStart(60, TimeUnit.SECONDS)) {
      Assert.fail("connector did not start within timeout");
    }
    return connectorTask;
  }
}
