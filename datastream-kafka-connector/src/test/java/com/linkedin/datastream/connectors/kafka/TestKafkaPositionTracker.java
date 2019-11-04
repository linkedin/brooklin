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
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.diag.KafkaPositionKey;
import com.linkedin.datastream.common.diag.KafkaPositionValue;
import com.linkedin.datastream.kafka.factory.KafkaConsumerFactory;
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
  private static final long POLL_TIMEOUT_MS = Duration.ofMillis(1_000).toMillis();

  private static final TopicPartition DEFAULT_TEST_TOPIC_PARTITION = new TopicPartition("cloves", 0);
  private static final Set<TopicPartition> DEFAULT_TEST_ASSIGNMENT = Collections.singleton(DEFAULT_TEST_TOPIC_PARTITION);

  private DatastreamTaskImpl _datastreamTask;
  private KafkaConnectorTask _connectorTask;
  private MockDatastreamEventProducer _producer;
  private KafkaConsumerState _state;
  private KafkaConsumer<byte[], byte[]> _consumer;
  private KafkaConsumerFactory<byte[], byte[]> _factory;

  @BeforeMethod(alwaysRun = true)
  public void beforeTestSetup() {
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
    when(_consumer.endOffsets(anyCollectionOf(TopicPartition.class), any(Duration.class))).thenAnswer(invocation -> {
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

  @AfterMethod(alwaysRun = true)
  public void afterTestTeardown() {
    if (_connectorTask != null) {
      _connectorTask.stop();
    }
  }

  /**
   * Base test of consumer position data being updated when events are received.
   */
  @Test
  public void testPollWithEvents() {
    _state = new KafkaConsumerState(DEFAULT_TEST_ASSIGNMENT);

    _connectorTask = createKafkaConnectorTask();
    _connectorTask.onPartitionsAssigned(DEFAULT_TEST_ASSIGNMENT);

    produceAndPollEvent(DEFAULT_TEST_TOPIC_PARTITION, Instant.ofEpochMilli(5));

    testConsumerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 1, Instant.ofEpochMilli(5));
    testBrokerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 1);

    produceAndPollEvent(DEFAULT_TEST_TOPIC_PARTITION, Instant.ofEpochMilli(6));

    testConsumerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 2, Instant.ofEpochMilli(6));
    testBrokerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 2);
  }

  /**
   * Tests that the consumer position data will update if the datastream is caught up even if it hasn't received events.
   */
  @Test
  public void testCaughtUpPoll() {
    _state = new KafkaConsumerState(DEFAULT_TEST_ASSIGNMENT);

    // Produce and expose the consumer to the events before we start the connector task up, so the consumer task will
    // load a caught up checkpoint.
    _state.push(DEFAULT_TEST_TOPIC_PARTITION, Instant.ofEpochMilli(5));
    _state.allowNextPoll();
    _state.poll();

    _connectorTask = createKafkaConnectorTask();
    _connectorTask.onPartitionsAssigned(DEFAULT_TEST_ASSIGNMENT);

    _state.allowNextPoll();
    updateBrokerPositionData();

    testConsumerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 1);
    testBrokerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 1);
  }

  /**
   * Tests that the consumer position data won't update for a single partition it can't get results for (but correctly
   * updates position data for other partitions it is getting data for).
   */
  @Test
  public void testPollWithMissingPartition() {
    TopicPartition faultyPartition = DEFAULT_TEST_TOPIC_PARTITION;
    TopicPartition goodPartition = new TopicPartition(DEFAULT_TEST_TOPIC_PARTITION.topic(),
        DEFAULT_TEST_TOPIC_PARTITION.partition() + 1);
    Set<TopicPartition> assignment = ImmutableSet.of(faultyPartition, goodPartition);
    _state = new KafkaConsumerState(assignment);

    _connectorTask = createKafkaConnectorTask();
    _connectorTask.onPartitionsAssigned(assignment);

    produceAndPollEvents(ImmutableMap.of(
        faultyPartition, Instant.ofEpochMilli(5),
        goodPartition, Instant.ofEpochMilli(6)));

    _state.setDisabledPartitions(faultyPartition);

    produceAndPollEvents(ImmutableMap.of(
        faultyPartition, Instant.ofEpochMilli(7),
        goodPartition, Instant.ofEpochMilli(8)));

    testConsumerPositionData(faultyPartition, 1, Instant.ofEpochMilli(5));
    testBrokerPositionData(faultyPartition, 2);
    testConsumerPositionData(goodPartition, 2, Instant.ofEpochMilli(8));
    testBrokerPositionData(goodPartition, 2);
  }

  /**
   * Tests that the consumer position data won't update if the consumer is unable to fetch events.
   */
  @Test
  public void testPollNoEvents() {
    _state = new KafkaConsumerState(DEFAULT_TEST_ASSIGNMENT);

    _connectorTask = createKafkaConnectorTask();
    _connectorTask.onPartitionsAssigned(DEFAULT_TEST_ASSIGNMENT);

    produceAndPollEvent(DEFAULT_TEST_TOPIC_PARTITION, Instant.ofEpochMilli(5));

    _state.setDisabledPartitions(DEFAULT_TEST_TOPIC_PARTITION);
    produceAndPollEvent(DEFAULT_TEST_TOPIC_PARTITION, Instant.ofEpochMilli(6));

    testConsumerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 1, Instant.ofEpochMilli(5));
    testBrokerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 2);
  }

  /**
   * Tests that the consumer position data won't update if the consumer is non-functional (polls throw exceptions and
   * consumer metric data is inaccurate).
   */
  @Test
  public void testPollWithException() {
    _state = new KafkaConsumerState(DEFAULT_TEST_ASSIGNMENT);

    _connectorTask = createKafkaConnectorTask();
    _connectorTask.onPartitionsAssigned(DEFAULT_TEST_ASSIGNMENT);

    produceAndPollEvent(DEFAULT_TEST_TOPIC_PARTITION, Instant.ofEpochMilli(5));

    _state.makeFaulty();
    produceAndPollEvent(DEFAULT_TEST_TOPIC_PARTITION, Instant.ofEpochMilli(6));

    testConsumerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 1, Instant.ofEpochMilli(5));
    testBrokerPositionData(DEFAULT_TEST_TOPIC_PARTITION, 2);
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
    positionTracker.queryBrokerForLatestOffsets(_consumer, _consumer.assignment(), Duration.ofSeconds(30));
  }

  /**
   * Produces an event to the specified topic partition with the timestamp provided, and then has the consumer poll it.
   * @param topicPartition the specified topic partition
   * @param timestamp the timestamps to use when producing the event
   */
  private void produceAndPollEvent(TopicPartition topicPartition, Instant timestamp) {
    produceAndPollEvents(ImmutableMap.of(topicPartition, timestamp));
  }

  /**
   * Produces events per the event data provided: a map of TopicPartition to the timestamp of the message to produce,
   * and then has the consumer poll it.
   * @param events the event data provided
   */
  private void produceAndPollEvents(Map<TopicPartition, Instant> events) {
    events.forEach((topicPartition, timestamp) -> _state.push(topicPartition, timestamp));
    updateBrokerPositionData();
    _state.allowNextPoll();

    PollUtils.poll(() -> {
      // Ensure all events are produced
      for (Map.Entry<TopicPartition, Instant> entry : events.entrySet()) {
        TopicPartition topicPartition = entry.getKey();
        Instant timestamp = entry.getValue();
        if (_state.getFaultyTopicPartitions().contains(topicPartition)) {
          continue; // Don't wait for topics that will not be produced to show up
        }
        boolean eventProduced = _producer.getEvents()
            .stream()
            .flatMap(record -> record.getEvents().stream())
            .anyMatch(event ->
                event.getMetadata().get("kafka-origin-partition").equals(String.valueOf(topicPartition.partition()))
                    && event.getMetadata().get("SourceTimestamp").equals(String.valueOf(timestamp.toEpochMilli())));
        LOG.info("Checking for event with partition {} with timestamp {}", topicPartition, timestamp);
        if (!eventProduced) {
          LOG.info("Event with for partition {} with timestamp {} has not been produced yet", topicPartition, timestamp);
          return false;
        }
      }
      LOG.info("All events successfully produced");
      return true;
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
  }

  /**
   * Tests that the consumer position data matches our expected position data.
   * @param topicPartition the topic partition to test position data for
   * @param expectedConsumerOffset the expected consumer offset
   */
  private void testConsumerPositionData(TopicPartition topicPartition, long expectedConsumerOffset) {
    testConsumerPositionData(topicPartition, expectedConsumerOffset, null);
  }

  /**
   * Tests that the consumer position data matches our expected position data.
   * @param topicPartition the topic partition to test position data for
   * @param expectedConsumerOffset the expected consumer offset
   * @param expectedRecordTimestamp the expected record timestamp
   */
  private void testConsumerPositionData(TopicPartition topicPartition, long expectedConsumerOffset,
      Instant expectedRecordTimestamp) {
    // Wait for data to be available
    PollUtils.poll(() -> {
      Optional<KafkaPositionValue> positionData = getPositionData(topicPartition);
      boolean available = positionData.isPresent()
          && positionData.get().getConsumerOffset() != null
          && (expectedRecordTimestamp == null || positionData.get().getLastRecordReceivedTimestamp() != null);
      if (!available) {
        LOG.info("Waiting for consumer position data to be set/available");
      } else {
        LOG.info("Consumer position data is set/available");
      }
      return available;
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    // Test data
    Optional<KafkaPositionValue> positionData = getPositionData(topicPartition);
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getConsumerOffset());
    Assert.assertEquals(positionData.get().getConsumerOffset().longValue(), expectedConsumerOffset);
    if (expectedRecordTimestamp != null) {
      Assert.assertNotNull(positionData.get().getLastRecordReceivedTimestamp());
      Assert.assertEquals(positionData.get().getLastRecordReceivedTimestamp(), expectedRecordTimestamp);
    }
  }

  /**
   * Tests that the broker position data matches our expected position data.
   * @param topicPartition the topic partition to test position data for
   * @param expectedBrokerOffset the expected broker offset
   */
  private void testBrokerPositionData(TopicPartition topicPartition, long expectedBrokerOffset) {
    // Wait for data to be available
    PollUtils.poll(() -> {
      Optional<KafkaPositionValue> positionData = getPositionData(topicPartition);
      boolean available = positionData.isPresent() && positionData.get().getBrokerOffset() != null;
      if (!available) {
        LOG.info("Waiting for broker position data to be set/available");
      } else {
        LOG.info("Broker position data is set/available");
      }
      return available;
    }, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    // Test data
    Optional<KafkaPositionValue> positionData = getPositionData(topicPartition);
    Assert.assertTrue(positionData.isPresent());
    Assert.assertNotNull(positionData.get().getBrokerOffset());
    Assert.assertEquals(positionData.get().getBrokerOffset().longValue(), expectedBrokerOffset);
  }

  /**
   * Creates and starts a KafkaConnectorTask
   * @return a started KafkaConnectorTask
   */
  private KafkaConnectorTask createKafkaConnectorTask() {
    final KafkaBasedConnectorConfig connectorConfig = new KafkaBasedConnectorConfigBuilder()
        .setConsumerFactory(_factory)
        .build();
    KafkaConnectorTask connectorTask = new KafkaConnectorTask(connectorConfig, _datastreamTask, "",
        new KafkaGroupIdConstructor(false, "testCluster"));
    final Thread consumerThread = new Thread(connectorTask, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.setUncaughtExceptionHandler((t, e) -> LOG.error("Got uncaught exception in consumer thread", e));
    consumerThread.start();
    return connectorTask;
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
    void setDisabledPartitions(TopicPartition... topicPartitions) {
      if (topicPartitions == null) {
        _disabledTopicPartitions = Collections.emptySet();
      } else {
        _disabledTopicPartitions = new HashSet<>(Arrays.asList(topicPartitions));
      }
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
     * Gets all topic partitions which are working.
     * @return all topic partitions which are working
     */
    Set<TopicPartition> getWorkingTopicPartitions() {
      return getAllTopicPartitions()
          .stream()
          .filter(tp -> !_faulty) // if we are faulty, then no topic partitions match
          .filter(tp -> !_disabledTopicPartitions.contains(tp))
          .collect(Collectors.toSet());
    }

    /**
     * Gets all topic partitions which are faulty.
     * @return all topic partitions which are faulty
     */
    Set<TopicPartition> getFaultyTopicPartitions() {
      return Sets.difference(getAllTopicPartitions(), getWorkingTopicPartitions());
    }

    /**
     * Mocks adding a Kafka event to a broker for consumption by a mocked Kafka consumer.
     * @param topicPartition the topic partition to add the event for
     * @param timestamp the timestamp of the event
     */
    synchronized void push(TopicPartition topicPartition, Instant timestamp) {
      ConsumerRecord<byte[], byte[]> consumerRecord =
          new ConsumerRecord<>(topicPartition.topic(), topicPartition.partition(),
              _recordsOnBroker.get(topicPartition).size(), timestamp.toEpochMilli(), TimestampType.LOG_APPEND_TIME,
              0L, // Does the checksum matter?
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