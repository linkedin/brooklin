package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.Charsets;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.connectors.kafka.BaseKafkaZkTest;
import com.linkedin.datastream.connectors.kafka.KafkaConsumerFactoryImpl;
import com.linkedin.datastream.connectors.kafka.MockDatastreamEventProducer;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTaskImpl;


public class TestKafkaMirrorMakerConnectorTask extends BaseKafkaZkTest {


  public Properties getKafkaProducerProperties() {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaCluster.getBrokers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 100);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

    return props;
  }

  private void produceEvents(String topic, int numEvents) {
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(getKafkaProducerProperties())) {
      for (int i = 0; i < numEvents; i++) {
        producer.send(
            new ProducerRecord<>(topic, ("key-" + i).getBytes(Charsets.UTF_8), ("value-" + i).getBytes(Charsets.UTF_8)),
            (metadata, exception) -> {
              if (exception != null) {
                throw new RuntimeException("Failed to send message.", exception);
              }
            });
      }
      producer.flush();
    }
  }

  private void createTopic(ZkUtils zkUtils, String topic) {
    if (!AdminUtils.topicExists(zkUtils, topic)) {
      AdminUtils.createTopic(zkUtils, topic, 1, 2, new Properties(), null);
    }
  }

  @Test
  public void testConsumeFromMultipleTopics() throws Exception {
    String yummyTopic = "YummyPizza";
    String saltyTopic = "SaltyPizza";
    String saladTopic = "HealthySalad";

    createTopic(_zkUtils, saladTopic);
    createTopic(_zkUtils, yummyTopic);
    createTopic(_zkUtils, saltyTopic);

    // create a datastream to consume from topics ending in "Pizza"
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    Datastream datastream =
        TestKafkaMirrorMakerConnector.createDatastream("pizzaStream", _broker, "\\w+Pizza", metadata);
    datastream.setTransportProviderName("default");
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString("%s");
    datastream.setDestination(destination);

    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    MockDatastreamEventProducer datastreamProducer = new MockDatastreamEventProducer();
    task.setEventProducer(datastreamProducer);

    KafkaMirrorMakerConnectorTask connectorTask = createKafkaMirrorMakerConnectorTask(task);

    // produce an event to each of the 3 topics
    produceEvents(yummyTopic, 1);
    produceEvents(saltyTopic, 1);
    produceEvents(saladTopic, 1);

    if (!PollUtils.poll(() -> datastreamProducer.getEvents().size() == 2, 100, 25000)) {
      Assert.fail("did not transfer the msgs within timeout. transferred " + datastreamProducer.getEvents().size());
    }

    List<DatastreamProducerRecord> records = datastreamProducer.getEvents();
    for (DatastreamProducerRecord record : records) {
      String destinationTopic = record.getDestination().get();
      Assert.assertTrue(destinationTopic.endsWith("Pizza"),
          "Unexpected event consumed from Datastream and sent to topic: " + destinationTopic);
    }

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStop(5000, TimeUnit.MILLISECONDS), "did not shut down on time");
  }

  private KafkaMirrorMakerConnectorTask createKafkaMirrorMakerConnectorTask(DatastreamTaskImpl task)
      throws InterruptedException {
    KafkaMirrorMakerConnectorTask connectorTask =
        new KafkaMirrorMakerConnectorTask(new KafkaConsumerFactoryImpl(), new Properties(), task, 1000,
            Duration.ofSeconds(0), 5);
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
