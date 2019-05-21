/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.diag.KafkaPositionKey;
import com.linkedin.datastream.common.diag.KafkaPositionValue;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTaskImpl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyCollectionOf;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.when;


/**
 * Tests the position data set by KafkaPositionTracker for a Kafka connector task.
 *
 * @see KafkaPositionTracker for the class which modifies/sets the position data
 * @see KafkaPositionKey for information on the PositionKey type used for Kafka
 * @see KafkaPositionValue for information on the PositionValue type used for Kafka
 */
@Test(singleThreaded = true)
public class TestKafkaPositionTracker {
  private static final Logger LOG = LoggerFactory.getLogger(TestKafkaPositionTracker.class);
  private static final long POLL_PERIOD_MS = Duration.ofMillis(250).toMillis();
  private static final long POLL_TIMEOUT_MS = Duration.ofMillis(250).toMillis();

  private DatastreamTaskImpl _datastreamTask;
  private KafkaConnectorTask _connectorTask;
  private MockDatastreamEventProducer _producer;
  private KafkaConsumerState _state;
  private KafkaConsumer<byte[], byte[]> _consumer;
  private KafkaConsumerFactory<byte[], byte[]> _factory;

  @BeforeMethod(alwaysRun = true)
  public void beforeMethodSetup() {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    Datastream datastream = new Datastream();
    datastream.setName("ChicagoStylePizza");
    datastream.setConnectorName("CrushedTomatoes");
    datastream.setTransportProviderName("DriedOregano");
    datastream.setMetadata(new StringMap());
    datastream.getMetadata().put(DatastreamMetadataConstants.TASK_PREFIX, "MozzarellaCheese");
    datastream.setSource(new DatastreamSource().setConnectionString("kafka://garlic:12345/cloves").setPartitions(2));
    datastream.setDestination(
        new DatastreamDestination().setConnectionString("kafka://crushed:12345/red_pepper_flakes").setPartitions(2));
    _datastreamTask = new DatastreamTaskImpl(Collections.singletonList(datastream));
    _producer = new MockDatastreamEventProducer();
    _datastreamTask.setEventProducer(_producer);
    _consumer = Mockito.mock(ByteBasedKafkaConsumer.class);
    when(_consumer.poll(anyLong())).thenAnswer(invocation -> _state.poll());
    when(_consumer.assignment()).thenAnswer(invocation -> _state.getAllTopicPartitions());
    when(_consumer.position(any(TopicPartition.class))).thenAnswer(invocation -> {
      TopicPartition topicPartition = invocation.getArgumentAt(0, TopicPartition.class);
      return _state.getConsumerOffsets(Collections.singleton(topicPartition)).get(topicPartition);
    });
    when(_consumer.endOffsets(anyCollectionOf(TopicPartition.class))).thenAnswer(invocation -> {
      Collection<?> topicPartitionsRaw = invocation.getArgumentAt(0, Collection.class);
      Set<TopicPartition> topicPartitions =
          topicPartitionsRaw.stream().map(object -> (TopicPartition) object).collect(Collectors.toSet());
      return _state.getBrokerOffsets(topicPartitions);
    });
    when(_consumer.metrics()).thenAnswer(invocation -> _state.getMetricsConsumerLag(_state.getAllTopicPartitions())
        .entrySet()
        .stream()
        .map(e -> new Metric() {
          @Override
          public MetricName metricName() {
            Map<String, String> tags = new HashMap<>();
            tags.put("topic", e.getKey().topic());
            tags.put("partition", String.valueOf(e.getKey().partition()));
            tags.put("client-id", "MeltedCheese");
            return new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags);
          }

          @Override
          public double value() {
            return e.getValue().doubleValue();
          }

          @Override
          public Object metricValue() {
            return e.getValue().doubleValue();
          }
        })
        .collect(Collectors.toMap(metric -> metric.metricName(), metric -> metric)));
    _factory = properties -> _consumer;
  }

  /**
   * Base test of consumer position data being updated when events are received.
   */
  @Test
  public void testPollWithEvents() {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _connectorTask = createKafkaConnectorTask();
    Thread consumerThread = new Thread(_connectorTask, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _connectorTask.onPartitionsAssigned(assignments);
    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Optional<KafkaPositionValue> positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(5L));

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(6L));

    _connectorTask.stop();
  }

  /**
   * Tests that the consumer position data will update if the datastream is caught up even if it hasn't received events.
   */
  @Test
  public void testCaughtUpPoll() {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);
    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);
    _state.allowNextPoll();
    _state.poll();

    _connectorTask = createKafkaConnectorTask();
    Thread consumerThread = new Thread(_connectorTask, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _connectorTask.onPartitionsAssigned(assignments);

    _state.allowNextPoll();
    PollUtils.poll(() ->
        getPositionData(new TopicPartition("cloves", 0)).isPresent()
            && getPositionData(new TopicPartition("cloves", 1)).isPresent(), POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Optional<KafkaPositionValue> positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);

    updateBrokerPositionData();

    positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 1L);

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 1L);

    _connectorTask.stop();
  }

  /**
   * Tests that the consumer position data won't update for a single partition it can't get results for (but correctly
   * updates position data for other partitions it is getting data for).
   */
  @Test
  public void testPollWithMissingPartition() {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _connectorTask = createKafkaConnectorTask();
    Thread consumerThread = new Thread(_connectorTask, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _connectorTask.onPartitionsAssigned(assignments);
    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    _state.push(new TopicPartition("cloves", 0), 7);
    _state.push(new TopicPartition("cloves", 1), 8);
    _state.setDisabledPartitions(Collections.singleton(new TopicPartition("cloves", 0)));

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 1, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Optional<KafkaPositionValue> positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(5L));

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 2L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(8L));

    updateBrokerPositionData();

    positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 2L);

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 2L);

    _connectorTask.stop();
  }

  /**
   * Tests that the consumer position data won't update if the consumer is unable to fetch events.
   */
  @Test
  public void testPollNoEvents() {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _connectorTask = createKafkaConnectorTask();
    Thread consumerThread = new Thread(_connectorTask, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _connectorTask.onPartitionsAssigned(assignments);
    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    _state.push(new TopicPartition("cloves", 0), 7);
    _state.push(new TopicPartition("cloves", 1), 8);
    _state.setDisabledPartitions(assignments);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 0, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Optional<KafkaPositionValue> positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(5L));

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(6L));

    updateBrokerPositionData();

    positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 2L);

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 2L);

    _connectorTask.stop();
  }

  /**
   * Tests that the consumer position data won't update if the consumer is non-functional (polls throw exceptions and
   * consumer metric data is inaccurate).
   */
  @Test
  public void testPollWithException() {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _connectorTask = createKafkaConnectorTask();
    Thread consumerThread = new Thread(_connectorTask, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _connectorTask.onPartitionsAssigned(assignments);
    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    _state.push(new TopicPartition("cloves", 0), 7);
    _state.push(new TopicPartition("cloves", 1), 8);
    _state.makeFaulty();

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 0, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Optional<KafkaPositionValue> positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(5L));

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), 1L);
    Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
    Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), Instant.ofEpochMilli(6L));

    updateBrokerPositionData();

    positionData = getPositionData(new TopicPartition("cloves", 0));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 2L);

    positionData = getPositionData(new TopicPartition("cloves", 1));
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), 2L);

    _connectorTask.stop();
  }

  /**
   * Gets the current position of the Connector's consumer for a given TopicPartition for a given Datastream group.
   *
   * @param topicPartition the TopicPartition that the Connector for this task prefix is reading from
   * @return a KafkaPositionValue containing position data, if present
   */
  private Optional<KafkaPositionValue> getPositionData(TopicPartition topicPartition) {
    final Map<KafkaPositionKey, KafkaPositionValue> positionData = _connectorTask.getKafkaPositionTracker()
            .map(KafkaPositionTracker::getPositions)
            .orElse(Collections.emptyMap());
    return positionData.entrySet()
        .stream()
        .filter(e -> e.getKey().getTopic().equals(topicPartition.topic()))
        .filter(e -> e.getKey().getPartition() == topicPartition.partition())
        .map(Map.Entry::getValue)
        .findAny();
  }

  /**
   * Updates the broker position data on the KafkaPositionTracker by querying the latest offsets on the broker for our
   * assigned topic partitions.
   */
  private void updateBrokerPositionData() {
    final KafkaPositionTracker positionTracker = _connectorTask.getKafkaPositionTracker()
        .orElseThrow(() -> new RuntimeException("Position tracker was not instantiated"));
    positionTracker.queryBrokerForLatestOffsets(_consumer, _consumer.assignment());
  }

  private KafkaConnectorTask createKafkaConnectorTask() {
    final KafkaBasedConnectorConfig connectorConfig = new KafkaBasedConnectorConfigBuilder()
        .setConsumerFactory(_factory)
        .build();
    return new KafkaConnectorTask(connectorConfig, _datastreamTask, "",
        new KafkaGroupIdConstructor(false, "testCluster"));
  }

  /**
   * Mocks the state of a Kafka consumer.
   */
  private static class KafkaConsumerState {
    /*
     * Consumer state
     */
    final Map<TopicPartition, Long> _lastPolledOffsets = new HashMap<>();
    final Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> _recordsOnBroker = new HashMap<>();

    /*
     * Lock variables for blocking poll()
     */
    private final Lock _lock = new ReentrantLock();
    private final Condition _signaled = _lock.newCondition();
    private volatile boolean _signal = false;

    /*
     * Consumer test settings
     */
    private volatile boolean _faulty = false;
    private Set<TopicPartition> _disabledTopicPartitions = new HashSet<>();

    /**
     * Initializes a Kafka consumer with specified topic partitions.
     * @param topicPartitions the specified topic partitions
     */
    KafkaConsumerState(Set<TopicPartition> topicPartitions) {
      topicPartitions.forEach(topicPartition -> {
        _lastPolledOffsets.put(topicPartition, 0L);
        _recordsOnBroker.put(topicPartition, new ArrayList<>());
      });
    }

    /**
     * Configures a set of topic partition such that poll() will not return any results from them.
     * @param topicPartitions the set of topic partitions
     */
    void setDisabledPartitions(Set<TopicPartition> topicPartitions) {
      _disabledTopicPartitions = topicPartitions;
    }

    /**
     * Ensures that all future poll() calls from this object will throw an exception, and that all metrics calls will
     * return incorrect results.
     */
    void makeFaulty() {
      _faulty = true;
    }

    /**
     * Gets the broker's current offset (high-water mark) for the specified topic partitions.
     * @param topicPartitions the specific topic partitions
     * @return a mapping from topic partitions to the broker's current offset (high-water mark)
     */
    Map<TopicPartition, Long> getBrokerOffsets(Set<TopicPartition> topicPartitions) {
      return _recordsOnBroker.entrySet()
          .stream()
          .filter(pOffset -> topicPartitions.contains(pOffset.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, pOffset -> (long) pOffset.getValue().size()));
    }

    /**
     * Gets the consumer's current offsets for the specified topic partitions.
     * @param topicPartitions the specified topic partitions
     * @return a mapping from topic partitions to the consumers's current offset
     */
    Map<TopicPartition, Long> getConsumerOffsets(Set<TopicPartition> topicPartitions) {
      return _lastPolledOffsets.entrySet()
          .stream()
          .filter(pOffset -> topicPartitions.contains(pOffset.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    Map<TopicPartition, Long> getMetricsConsumerLag(Set<TopicPartition> topicPartitions) {
      Map<TopicPartition, Long> brokerOffsets = getBrokerOffsets(topicPartitions);
      Map<TopicPartition, Long> consumerOffsets = getConsumerOffsets(topicPartitions);
      Map<TopicPartition, Long> consumerLag = brokerOffsets.keySet().stream()
          .collect(Collectors.toMap(tp -> tp, tp -> brokerOffsets.get(tp) - consumerOffsets.getOrDefault(tp, 0L)));

      // Use invalid results for faulty topic partitions
      getFaultyTopicPartitions().forEach(tp -> consumerLag.put(tp, 0L));

      return consumerLag;
    }

    /**
     * Gets all topic partitions for this consumer state.
     * @return all topic partitions
     */
    Set<TopicPartition> getAllTopicPartitions() {
      return _recordsOnBroker.keySet();
    }

    /**
     * Gets all topic partitions which are configured to be faulty.
     * @return all topic partitions which are faulty
     */
    Set<TopicPartition> getFaultyTopicPartitions() {
      return getAllTopicPartitions()
          .stream()
          .filter(tp -> !_disabledTopicPartitions.contains(tp))
          .filter(tp -> !_faulty)
          .collect(Collectors.toSet());
    }

    /**
     * Mocks adding a Kafka event to a broker for consumption by a mocked Kafka consumer.
     * @param topicPartition the topic partition to add the event for
     * @param timestamp the timestamp of the event
     */
    synchronized void push(TopicPartition topicPartition, long timestamp) {
      ConsumerRecord<byte[], byte[]> consumerRecord =
          new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(),
              _recordsOnBroker.get(topicPartition).size(), timestamp, TimestampType.LOG_APPEND_TIME, 0L,
              // Does the checksum matter?
              topicPartition.toString().length(), // Serialized Key Size
              topicPartition.toString().length(), // Serialized Value Size
              topicPartition.toString().getBytes(), // Key
              topicPartition.toString().getBytes() // Value
          );
      List<ConsumerRecord<byte[], byte[]>> replacementList = new ArrayList<>(_recordsOnBroker.get(topicPartition));
      replacementList.add(consumerRecord);
      _recordsOnBroker.put(topicPartition, replacementList);
      LOG.info("Pushing an event with timestamp " + timestamp + " to " + topicPartition);
      LOG.info("Current broker offsets: " + getBrokerOffsets(getAllTopicPartitions()));
    }

    /**
     * Returns what would be returned by a poll() if invoked by a Kafka consumer. Blocked until allowNextPoll() is
     * called.
     * @return the result of a poll() operation on a KafkaConsumer
     */
    ConsumerRecords<byte[], byte[]> poll() {
      try {
        _lock.lock();
        while (!_signal) {
          _signaled.await();
        }
        return pollImpl(_lastPolledOffsets);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        _signal = false;
        _lock.unlock();
      }
    }

    private synchronized ConsumerRecords<byte[], byte[]> pollImpl(Map<TopicPartition, Long> fromOffsets) {
      if (_faulty) {
        throw new RuntimeException("This exception is expected and as is part of a test. :)");
      }
      Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> results = fromOffsets.entrySet()
          .stream()
          .filter(pOffset -> !_disabledTopicPartitions.contains(pOffset.getKey()))
          .collect(Collectors.toMap(Map.Entry::getKey, pOffset -> {
            List<ConsumerRecord<byte[], byte[]>> data = _recordsOnBroker.get(pOffset.getKey());
            data = data.subList(Math.toIntExact(pOffset.getValue()), data.size());
            return data;
          }));
      fromOffsets.keySet()
          .stream()
          .filter(topicPartition -> !_disabledTopicPartitions.contains(topicPartition))
          .forEach(topicPartition -> _lastPolledOffsets.put(topicPartition,
              (long) _recordsOnBroker.get(topicPartition).size()));
      Map<TopicPartition, Integer> resultCount =
          results.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().size()));
      LOG.info("Tried to poll for " + fromOffsets.keySet() + " and got " + resultCount.toString() + " results");
      return new ConsumerRecords<>(results.entrySet()
          .stream()
          .filter(e -> !e.getValue().isEmpty())
          .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)));
    }

    /**
     * Allows the next poll to occur.
     */
    void allowNextPoll() {
      try {
        _lock.lock();
        _signal = true;
        _signaled.signal();
      } finally {
        _lock.unlock();
      }
    }
  }

  /**
   * This class exists to get around unchecked exceptions when trying to mock KafkaConsumer.class with non-generic
   * arguments.
   */
  private static class ByteBasedKafkaConsumer extends KafkaConsumer<byte[], byte[]> {
    public ByteBasedKafkaConsumer(Properties properties) {
      super(properties);
    }
  }
}