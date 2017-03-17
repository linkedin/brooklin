package com.linkedin.datastream.connectors.kafka;

import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.kafka.EmbeddedZookeeperKafkaCluster;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTaskImpl;


public class TestKafkaConnectorTask {
  private EmbeddedZookeeperKafkaCluster _kafkaCluster;

  @BeforeTest
  public void setup() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry());
    _kafkaCluster = new EmbeddedZookeeperKafkaCluster();
    _kafkaCluster.startup();
  }

  @AfterTest
  public void teardown() throws Exception {
    _kafkaCluster.shutdown();
  }

  @Test
  public void testConsume() throws Exception {
    String topic = "pizza";
    String broker = _kafkaCluster.getBrokers().split("\\s*,\\s*")[0];

    //produce 100 msgs to topic before start

    Properties props = new Properties();
    props.put("bootstrap.servers", broker);
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", ByteArraySerializer.class.getCanonicalName());
    props.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 0; i < 100; i++) {
        producer.send(new ProducerRecord<>(topic, ("key-" + i).getBytes("UTF-8"), ("value-" + i).getBytes("UTF-8")));
      }
      producer.flush();
    }

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
    DatastreamTaskImpl task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    task.setEventProducer(datastreamProducer);
    KafkaConnectorTask connectorTask =
        new KafkaConnectorTask(new KafkaConsumerFactoryImpl(), new Properties(), task, 100);
    Thread t = new Thread(connectorTask, "connector thread");
    t.setDaemon(true);
    t.setUncaughtExceptionHandler((t1, e) -> {
      Assert.fail("connector thread died", e);
    });
    t.start();
    if (!connectorTask.awaitStart(10, TimeUnit.SECONDS)) {
      Assert.fail("connector did not start within timeout");
    }

    //nothing of the 1st 100 msgs was delivered, as connector was started after delivery

    Thread.sleep(200);
    Assert.assertTrue(datastreamProducer.getEvents().isEmpty());

    //send 100 more msgs

    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(props)) {
      for (int i = 100; i < 200; i++) {
        producer.send(new ProducerRecord<>(topic, ("key-" + i).getBytes("UTF-8"), ("value-" + i).getBytes("UTF-8")));
      }
      producer.flush();
    }

    long timeout = System.currentTimeMillis() + 5000;
    while (datastreamProducer.getEvents().size() != 100) {
      if (System.currentTimeMillis() > timeout) {
        Assert.fail("did not transfer 100 msgs within timeout. transferred " + datastreamProducer.getEvents().size());
      }
      Thread.sleep(50);
    }

    Assert.assertEquals(100, datastreamProducer.getEvents().size());

    connectorTask.stop();
    Assert.assertTrue(connectorTask.awaitStart(5000, TimeUnit.MILLISECONDS), "did not shut down on time");
  }
}
