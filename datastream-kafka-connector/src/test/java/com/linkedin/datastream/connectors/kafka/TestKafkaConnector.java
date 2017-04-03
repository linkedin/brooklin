package com.linkedin.datastream.connectors.kafka;

import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.kafka.EmbeddedZookeeperKafkaCluster;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.api.connector.DatastreamValidationException;


public class TestKafkaConnector {

  private EmbeddedZookeeperKafkaCluster _kafkaCluster;
  private String _broker;

  @BeforeTest
  public void setup() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry());
    _kafkaCluster = new EmbeddedZookeeperKafkaCluster();
    _kafkaCluster.startup();
    _broker = _kafkaCluster.getBrokers().split("\\s*,\\s*")[0];
  }

  @AfterTest
  public void teardown() throws Exception {
    _kafkaCluster.shutdown();
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
  public void testConnectorPopulatesPartitions() throws UnsupportedEncodingException, DatastreamValidationException {
    String topicName = "testConnectorPopulatesPartitions";
    TestKafkaConnectorTask.produceEvents(_broker, topicName, 10);

    Datastream ds = createDatastream("testConnectorPopulatesPartitions", topicName);
    KafkaConnector connector = new KafkaConnector("test", 10000, new KafkaConsumerFactoryImpl(), new Properties(),
        Collections.emptyList());
    connector.initializeDatastream(ds, Collections.emptyList());
    Assert.assertEquals(ds.getSource().getPartitions().intValue(), 1);
  }

  @Test(expectedExceptions = DatastreamValidationException.class)
  public void testConnectorValidatesWhitelistedBroker() throws DatastreamValidationException {
    String topicName = "testConnectorValidatesWhitelistedBroker";

    Datastream ds = createDatastream("testConnectorPopulatesPartitions", topicName);
    KafkaConnector connector = new KafkaConnector("test", 10000, new KafkaConsumerFactoryImpl(), new Properties(),
        Collections.singletonList(KafkaBrokerAddress.valueOf("randomBroker:2546")));
    connector.initializeDatastream(ds, Collections.emptyList());
  }
}
