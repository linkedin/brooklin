package com.linkedin.datastream.connectors.kafka;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.JsonUtils;
import com.linkedin.datastream.common.zk.ZkClient;
import com.linkedin.datastream.kafka.EmbeddedZookeeperKafkaCluster;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTaskImpl;


public class TestKafkaConnectorTask {

  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaConnectorTask.class);

  private EmbeddedZookeeperKafkaCluster _kafkaCluster;
  private ZkUtils _zkUtils;

  @BeforeTest
  public void setup() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry());
    Properties kafkaConfig = new Properties();
    // we will disable auto topic creation for this test file
    kafkaConfig.setProperty("auto.create.topics.enable", Boolean.FALSE.toString());
    _kafkaCluster = new EmbeddedZookeeperKafkaCluster(kafkaConfig);
    _kafkaCluster.startup();
    _zkUtils =
        new ZkUtils(new ZkClient(_kafkaCluster.getZkConnection()), new ZkConnection(_kafkaCluster.getZkConnection()),
            false);
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
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
    props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());

    if (!AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, 1, 1, new Properties(), null);
    }
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < numEvents; i++) {
        final int finalIndex = index;
        producer.send(
            new ProducerRecord<>(topic, ("key-" + index).getBytes("UTF-8"), ("value-" + index).getBytes("UTF-8")),
            new Callback() {
              @Override
              public void onCompletion(RecordMetadata metadata, Exception exception) {
                LOG.info("send completed for event {} at offset {}", finalIndex, metadata.offset());
              }
            });
        index++;
      }
      producer.flush();
    }
  }

  @Test
  public void testConsume() throws Exception {
    String topic = "pizza";
    String broker = _kafkaCluster.getBrokers().split("\\s*,\\s*")[0];

    LOG.info("Sending first set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _zkUtils, topic, 0, 100);
    Map<Integer, Long> startOffsets = Collections.singletonMap(0, 100L);

    LOG.info("Sending second set of events");

    //produce 100 msgs to topic before start
    produceEvents(_kafkaCluster, _zkUtils, topic, 100, 100);

    //start
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("kafka://" + broker + "/pizza");
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString("whatever://bob");
    Datastream datastream = new Datastream();
    datastream.setName("bob");
    datastream.setConnectorName("whatever");
    datastream.setSource(source);
    datastream.setDestination(destination);
    datastream.setTransportProviderName("default");
    datastream.setMetadata(new StringMap());
    datastream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, DatastreamTaskImpl.getTaskPrefix(datastream));

    // Unable to set the start position, OffsetToTimestamp is returning null in the embedded kafka cluster.
    datastream.getMetadata().put(DatastreamMetadataConstants.START_POSITION, JsonUtils.toJson(startOffsets));
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);

    KafkaConnectorTask connectorTask =
        new KafkaConnectorTask(new KafkaConsumerFactoryImpl(), new Properties(), task, 1000);
    Thread t = new Thread(connectorTask, "connector thread");
    t.setDaemon(true);
    t.setUncaughtExceptionHandler((t1, e) -> {
      Assert.fail("connector thread died", e);
    });
    t.start();
    if (!connectorTask.awaitStart(60, TimeUnit.SECONDS)) {
      Assert.fail("connector did not start within timeout");
    }

    LOG.info("Sending third set of events");

    //send 100 more msgs
    produceEvents(_kafkaCluster, _zkUtils, topic, 1000, 100);

    long timeout = System.currentTimeMillis() + 5000;
    while (datastreamProducer.getEvents().size() != 200) {
      if (System.currentTimeMillis() > timeout) {
        Assert.fail("did not transfer 200 msgs within timeout. transferred " + datastreamProducer.getEvents().size());
      }
      Thread.sleep(50);
    }

    Assert.assertEquals(200, datastreamProducer.getEvents().size());

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStart(5000, TimeUnit.MILLISECONDS), "did not shut down on time");
  }
}
