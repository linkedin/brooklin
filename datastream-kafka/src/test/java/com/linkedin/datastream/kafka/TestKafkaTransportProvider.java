/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import kafka.admin.AdminUtils;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.server.api.transport.TransportProvider;
import com.linkedin.datastream.testutil.BaseKafkaZkTest;
import com.linkedin.datastream.testutil.DatastreamTestUtils;

import static org.mockito.Mockito.mock;


/**
 * Tests for {@link KafkaTransportProvider}.
 */
@Test
public class TestKafkaTransportProvider extends BaseKafkaZkTest {
  private static final Integer NUM_PARTITIONS = 10;
  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaTransportProvider.class);
  private static AtomicInteger _topicCounter = new AtomicInteger();

  private Properties _transportProviderProperties;

  @BeforeMethod
  public void setup() throws IOException  {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    _transportProviderProperties = new Properties();
    _transportProviderProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, _kafkaCluster.getBrokers());
    _transportProviderProperties.put(ProducerConfig.CLIENT_ID_CONFIG, "testClient");
    _transportProviderProperties.put(KafkaTransportProviderAdmin.ZK_CONNECT_STRING_CONFIG, _kafkaCluster.getZkConnection());
  }

  @Test
  public void testAssign() {
    String topicName = getUniqueTopicName();
    KafkaTransportProviderAdmin provider = new KafkaTransportProviderAdmin("test", _transportProviderProperties);
    int numberOfPartitions = 32;

    Datastream ds =
        DatastreamTestUtils.createDatastream("test", "ds1", "source",
            provider.getDestination(null, topicName), numberOfPartitions);
    Set<KafkaProducerWrapper<byte[], byte[]>> producers =
        IntStream.range(0, KafkaTransportProviderAdmin.DEFAULT_PRODUCERS_PER_CONNECTOR)
            .mapToObj(x -> ((KafkaTransportProvider) provider.assignTransportProvider(
                new DatastreamTaskImpl(Collections.singletonList(ds)))).getProducers().get(0))
            .collect(Collectors.toSet());

    Assert.assertEquals(producers.size(), KafkaTransportProviderAdmin.DEFAULT_PRODUCERS_PER_CONNECTOR);

    Set<KafkaProducerWrapper<byte[], byte[]>> producers2 =
        IntStream.range(0, KafkaTransportProviderAdmin.DEFAULT_PRODUCERS_PER_CONNECTOR)
            .mapToObj(x -> ((KafkaTransportProvider) provider.assignTransportProvider(
                new DatastreamTaskImpl(Collections.singletonList(ds)))).getProducers().get(0))
            .collect(Collectors.toSet());

    Assert.assertTrue(producers2.containsAll(producers));

    producers.forEach(x -> Assert.assertEquals(x.getTasksSize(), 2));
  }

  @Test
  public void testAssignMultipleProducers() {
    String topicName = getUniqueTopicName();
    KafkaTransportProviderAdmin provider = new KafkaTransportProviderAdmin("test", _transportProviderProperties);
    int numberOfPartitions = 32;

    Datastream ds =
        DatastreamTestUtils.createDatastream("test", "ds1", "source",
            provider.getDestination(null, topicName), numberOfPartitions);

    // Check that the default number of producers per task is 1
    List<KafkaProducerWrapper<byte[], byte[]>> producers = ((KafkaTransportProvider) provider.assignTransportProvider(
        new DatastreamTaskImpl(Collections.singletonList(ds)))).getProducers();
    Assert.assertEquals(producers.size(), 1);

    // Check now with an override of 3 producers per task
    ds.getMetadata().put(KafkaTransportProviderAdmin.CONFIG_PRODUCERS_PER_TASK, "3");
    producers = ((KafkaTransportProvider) provider.assignTransportProvider(
        new DatastreamTaskImpl(Collections.singletonList(ds)))).getProducers();
    Assert.assertEquals(producers.size(), 3);

    // Asking more producers than are in the pool
    ds.getMetadata().put(KafkaTransportProviderAdmin.CONFIG_PRODUCERS_PER_TASK, "3333");
    producers = ((KafkaTransportProvider) provider.assignTransportProvider(
        new DatastreamTaskImpl(Collections.singletonList(ds)))).getProducers();
    Assert.assertEquals(producers.size(), KafkaTransportProviderAdmin.DEFAULT_PRODUCERS_PER_CONNECTOR);

    // Now changing the default number of producers per task; and remove the override in the datastream.
    Properties prop2 = new Properties();
    prop2.putAll(_transportProviderProperties);
    prop2.put(KafkaTransportProviderAdmin.CONFIG_PRODUCERS_PER_TASK, "4");
    provider = new KafkaTransportProviderAdmin("test", prop2);
    ds.getMetadata().remove(KafkaTransportProviderAdmin.CONFIG_PRODUCERS_PER_TASK);
    producers = ((KafkaTransportProvider) provider.assignTransportProvider(
        new DatastreamTaskImpl(Collections.singletonList(ds)))).getProducers();
    Assert.assertEquals(producers.size(), 4);
  }

  @Test
  public void testReAssignBuggyProducer() throws Exception {
    String badDestUri = "kafka://badLocations:1234/Badtopic";
    String goodDestUri = "kafka://goodLocation:1234/goodtopic";

    _transportProviderProperties.remove(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

    List<DatastreamProducerRecord> datastreamEvents = Collections.singletonList(mock(DatastreamProducerRecord.class));
    KafkaTransportProviderAdmin provider = new KafkaTransportProviderAdmin("test", _transportProviderProperties);

    Datastream stream = DatastreamTestUtils.createDatastream("test", "testGetBrokers", "source", badDestUri, 2);
    provider.initializeDestinationForDatastream(stream, null);

    //Verify bad producer has been initiated and the send can be terminated upon unassigned

    DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(stream));
    KafkaTransportProvider transportProvider = (KafkaTransportProvider) provider.assignTransportProvider(task);
    transportProvider.getProducers().stream().
        forEach(p -> Assert.assertEquals("badLocations:1234", p.getProperties().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)));

    AtomicBoolean sentCompleted = new AtomicBoolean(false);
    Thread senderThread = new Thread(() -> {
      try {
        transportProvider.send(badDestUri, datastreamEvents.get(0), null);
      } catch (Exception ex) {

      }
      sentCompleted.set(true);
    });
    senderThread.start();

    provider.unassignTransportProvider(task);
    // Verify the sent will be forced to completed
    Assert.assertTrue(PollUtils.poll(() -> Boolean.TRUE.equals(sentCompleted.get()), 1000, 10000));

    //Verify the healthy broker can be initiated later
    Datastream stream2 = DatastreamTestUtils.createDatastream("test", "testGetBrokers", "source", goodDestUri, 2);

    Set<KafkaProducerWrapper<byte[], byte[]>> newProducers =
        IntStream.range(0, KafkaTransportProviderAdmin.DEFAULT_PRODUCERS_PER_CONNECTOR)
            .mapToObj(x -> ((KafkaTransportProvider) provider.assignTransportProvider(
                new DatastreamTaskImpl(Collections.singletonList(stream2)))).getProducers().get(0))
            .collect(Collectors.toSet());
    newProducers.stream().forEach(p ->
        Assert.assertEquals("goodLocation:1234", p.getProperties().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)));

  }

  @Test
  public void testSendHappyPath() throws Exception {
    testEventSend(1, 1, 0, true, true, "test");
  }

  @Test(enabled = true)
  public void testSendWithoutPartitionNumber() throws Exception {
    testEventSend(1, 2, -1, true, true, "test");
  }

  @Test(enabled = true)
  public void testEventWithoutKeyAndPartition() throws Exception {
    testEventSend(1, 2, -1, false, true, "test");
  }

  @Test
  public void testEventWithoutKeyNOrValue() throws Exception {
    testEventSend(1, 2, 0, false, false, "test");
  }

  @Test
  public void testEventWithoutKeyValueAndPartition() throws Exception {
    testEventSend(1, 2, -1, false, false, "test");
  }

  private void testEventSend(int numberOfEvents, int numberOfPartitions, int partition, boolean includeKey,
      boolean includeValue, String metricsPrefix) throws Exception {
    String topicName = getUniqueTopicName();
    final int[] eventsReceived = {0};

    if (metricsPrefix != null) {
      _transportProviderProperties.put(KafkaTransportProviderAdmin.CONFIG_METRICS_NAMES_PREFIX, metricsPrefix);
    }
    KafkaTransportProviderAdmin provider = new KafkaTransportProviderAdmin("test", _transportProviderProperties);

    String destinationUri = provider.getDestination(null, topicName);

    Datastream ds = DatastreamTestUtils.createDatastream("test", "ds1", "source", destinationUri, numberOfPartitions);

    DatastreamTask task = new DatastreamTaskImpl(Collections.singletonList(ds));
    TransportProvider transportProvider = provider.assignTransportProvider(task);
    provider.createTopic(destinationUri, numberOfPartitions, new Properties());

    Assert.assertTrue(PollUtils.poll(() -> AdminUtils.topicExists(_zkUtils, topicName), 1000, 10000));

    LOG.info(String.format("Topic %s created with %d partitions and topic properties %s", topicName, numberOfPartitions,
        new Properties()));
    List<DatastreamProducerRecord> datastreamEvents =
        createEvents(topicName, partition, numberOfEvents, includeKey, includeValue);

    LOG.info(String.format("Trying to send %d events to topic %s", datastreamEvents.size(), topicName));

    final Integer[] callbackCalled = {0};
    for (DatastreamProducerRecord event : datastreamEvents) {
      transportProvider.send(destinationUri, event, ((metadata, exception) -> callbackCalled[0]++));
    }

    // wait until all messages were acked, to ensure all events were successfully sent to the topic
    Assert.assertTrue(PollUtils.poll(() -> callbackCalled[0] == datastreamEvents.size(), 1000, 10000),
        "Send callback was not called; likely topic was not created in time");

    LOG.info(String.format("Trying to read events from the topicName %s partition %d", topicName, partition));

    Map<String, String> events = new HashMap<>();
    KafkaTestUtils.readTopic(topicName, partition, _kafkaCluster.getBrokers(), (key, value) -> {
      events.put(new String(key), new String(value));
      eventsReceived[0]++;
      return eventsReceived[0] < numberOfEvents;
    });

    if (metricsPrefix != null) {
      // verify that configured metrics prefix was used
      for (BrooklinMetricInfo metric : provider.getMetricInfos()) {
        Assert.assertTrue(metric.getNameOrRegex().startsWith(metricsPrefix));
      }

      String eventWriteRateMetricName = new StringJoiner(".").add(metricsPrefix)
          .add(KafkaTransportProvider.class.getSimpleName())
          .add(KafkaTransportProvider.AGGREGATE)
          .add(KafkaTransportProvider.EVENT_WRITE_RATE)
          .toString();

      String eventByteWriteRateMetricName = new StringJoiner(".").add(metricsPrefix)
          .add(KafkaTransportProvider.class.getSimpleName())
          .add(KafkaTransportProvider.AGGREGATE)
          .add(KafkaTransportProvider.EVENT_BYTE_WRITE_RATE)
          .toString();

      String producerCountMetricName = new StringJoiner(".").add(metricsPrefix)
          .add(KafkaProducerWrapper.class.getSimpleName())
          .add(KafkaTransportProvider.AGGREGATE)
          .add(KafkaProducerWrapper.PRODUCER_COUNT)
          .toString();
      Assert.assertNotNull(DynamicMetricsManager.getInstance().getMetric(eventWriteRateMetricName));
      Assert.assertNotNull(DynamicMetricsManager.getInstance().getMetric(eventByteWriteRateMetricName));
      Assert.assertNotNull(DynamicMetricsManager.getInstance().getMetric(producerCountMetricName));
    }
  }


  private byte[] createMessage(String text) {
    return text.getBytes();
  }

  private List<DatastreamProducerRecord> createEvents(String topicName, int partition, int numberOfEvents,
      boolean includeKey, boolean includeValue) {
    Datastream stream = new Datastream();
    stream.setName("datastream_" + topicName);
    stream.setConnectorName("dummyConnector");
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("SRC_" + topicName);
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString(topicName);
    destination.setPartitions(NUM_PARTITIONS);
    stream.setDestination(destination);

    List<DatastreamProducerRecord> events = new ArrayList<>();
    for (int index = 0; index < numberOfEvents; index++) {
      String key = "key" + index;
      byte[] payload = createMessage("payload " + index);
      byte[] previousPayload = createMessage("previousPayload " + index);

      byte[] keyValue = new byte[0];
      Object payloadValue = new byte[0];
      Object previousPayloadValue = new byte[0];

      if (includeKey) {
        keyValue = key.getBytes();
      }

      if (includeValue) {
          payloadValue = payload;
          previousPayloadValue = previousPayload;
      }

      DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
      builder.setEventsSourceTimestamp(System.currentTimeMillis());
      builder.addEvent(new BrooklinEnvelope(keyValue, payloadValue, previousPayloadValue, new HashMap<>()));
      if (partition >= 0) {
        builder.setPartition(partition);
      } else {
        builder.setPartitionKey(key);
      }

      builder.setSourceCheckpoint("test");
      events.add(builder.build());
    }

    return events;
  }


  private String getUniqueTopicName() {
    return "testTopic_" + _topicCounter.incrementAndGet();
  }
}
