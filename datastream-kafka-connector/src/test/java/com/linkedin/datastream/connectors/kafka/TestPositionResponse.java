/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
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
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.common.PollUtils;
import com.linkedin.datastream.common.diag.DatastreamPositionResponse;
import com.linkedin.datastream.common.diag.PhysicalSourcePosition;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamTaskImpl;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyCollectionOf;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.when;


/**
 * Tests that the position metadata in the Kafka connector task is being appropriately updated.
 * @see com.linkedin.datastream.common.diag.PhysicalSourcePosition for information about position metadata
 */
@Test
public class TestPositionResponse {

  private static final Logger LOG = LoggerFactory.getLogger(TestPositionResponse.class);
  private static final long POLL_PERIOD_MS = Duration.ofMillis(250).toMillis();
  private static final long POLL_TIMEOUT_MS = Duration.ofMillis(250).toMillis();

  private KafkaConnectorTask _consumer;
  private MockDatastreamEventProducer _producer;
  private KafkaConsumerState _state;
  private KafkaConsumerFactory<byte[], byte[]> _factory;
  private DatastreamTaskImpl _task;

  @BeforeMethod(alwaysRun = true)
  public void beforeMethodSetup() throws Exception {
    DynamicMetricsManager.createInstance(new MetricRegistry(), getClass().getSimpleName());
    Datastream datastream = new Datastream();
    datastream.setName("ChicagoStylePizza");
    datastream.setConnectorName("CrushedTomatoes");
    datastream.setTransportProviderName("DriedOregano");
    datastream.setMetadata(new StringMap());
    datastream.setSource(new DatastreamSource().setConnectionString("kafka://garlic:12345/cloves").setPartitions(2));
    datastream.setDestination(
        new DatastreamDestination().setConnectionString("kafka://crushed:12345/red_pepper_flakes").setPartitions(2));
    _task = new DatastreamTaskImpl(Collections.singletonList(datastream));
    _producer = new MockDatastreamEventProducer();
    _task.setEventProducer(_producer);
    KafkaConsumer<byte[], byte[]> consumer = Mockito.mock(ByteBasedKafkaConsumer.class);
    when(consumer.poll(anyLong())).thenAnswer(invocation -> _state.poll());
    when(consumer.assignment()).thenAnswer(invocation -> _state.getAllTopicPartitions());
    when(consumer.position(any(TopicPartition.class))).thenAnswer(invocation -> {
      TopicPartition topicPartition = invocation.getArgumentAt(0, TopicPartition.class);
      return _state.getConsumerOffsets(Collections.singleton(topicPartition)).get(topicPartition);
    });
    when(consumer.endOffsets(anyCollectionOf(TopicPartition.class))).thenAnswer(invocation -> {
      if (_state.isFaulty()) {
        return new HashMap<>();
      }
      Collection<?> topicPartitionsRaw = invocation.getArgumentAt(0, Collection.class);
      Set<TopicPartition> topicPartitions =
          topicPartitionsRaw.stream().map(object -> (TopicPartition) object).collect(Collectors.toSet());
      return _state.getBrokerOffsets(topicPartitions);
    });
    when(consumer.metrics()).thenAnswer(invocation -> _state.getMetricsConsumerLag(_state.getAllTopicPartitions())
        .entrySet()
        .stream()
        .map(e -> new Metric() {
          @Override
          public MetricName metricName() {
            Map<String, String> tags = new HashMap<>();
            tags.put("topic", e.getKey().topic());
            tags.put("partition", String.valueOf(e.getKey().partition()));
            return new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags);
          }

          @Override
          public Object metricValue() {
            return e.getValue();
          }

          @Override
          public double value() {
            return e.getValue().doubleValue();
          }
        })
        .collect(Collectors.toMap(metric -> metric.metricName(), metric -> metric)));
    _factory = properties -> consumer;
  }

  /**
   * Base test of consumer position data being updated when events are received.
   */
  @Test
  public void testPollWithEvents() throws Exception {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _consumer = new KafkaConnectorTask(
        new KafkaBasedConnectorConfig(_factory, null, new Properties(), "", "", 1000, 5, Duration.ZERO, false,
            Duration.ZERO), _task, "", new KafkaGroupIdConstructor(false, "testCluster"));
    Thread consumerThread = new Thread(_consumer, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    DatastreamPositionResponse response = _consumer.getPositionResponse();

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)), Optional.of(5L));
    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)), Optional.of(6L));

    _consumer.stop();
  }

  /**
   * Tests that the consumer position data will update if the datastream is caught up even if it hasn't received events.
   */
  @Test
  public void testCaughtUpPoll() throws Exception {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _consumer = new KafkaConnectorTask(
        new KafkaBasedConnectorConfig(_factory, null, new Properties(), "", "", 1000, 5, Duration.ZERO, false,
            Duration.ZERO), _task, "", new KafkaGroupIdConstructor(false, "testCluster"));
    Thread consumerThread = new Thread(_consumer, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    _state.allowNextPoll();
    PollUtils.poll(() -> false, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    long currentTime = System.currentTimeMillis();
    _consumer._kafkaPositionTracker.get().updateLatestBrokerOffsetsByRpc(_factory.createConsumer(null),
        _state.getAllTopicPartitions(), currentTime);
    DatastreamPositionResponse response = _consumer.getPositionResponse();

    Assert.assertEquals((long) getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)).orElse(0L), currentTime);

    Assert.assertEquals((long) getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)).orElse(0L), currentTime);

    _consumer.stop();
  }

  /**
   * Tests that the consumer position data won't update for a single partition it can't get results for (but correctly
   * updates position data for other partitions it is getting data for).
   */
  @Test
  public void testPollWithMissingPartition() throws Exception {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _consumer = new KafkaConnectorTask(
        new KafkaBasedConnectorConfig(_factory, null, new Properties(), "", "", 1000, 5, Duration.ZERO, false,
            Duration.ZERO), _task, "", new KafkaGroupIdConstructor(false, "testCluster"));
    Thread consumerThread = new Thread(_consumer, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    _state.push(new TopicPartition("cloves", 0), 7);
    _state.push(new TopicPartition("cloves", 1), 8);
    _state.setDisabledPartitions(Collections.singleton(new TopicPartition("cloves", 0)));

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 1, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    DatastreamPositionResponse response = _consumer.getPositionResponse();

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)), Optional.of(5L));
    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)), Optional.of(8L));

    _state.allowNextPoll();
    PollUtils.poll(() -> false, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    long currentTime = System.currentTimeMillis();
    _consumer._kafkaPositionTracker.get().updateLatestBrokerOffsetsByRpc(_factory.createConsumer(null),
        _state.getAllTopicPartitions(), currentTime);
    response = _consumer.getPositionResponse();

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)), Optional.of(5L));
    Assert.assertEquals((long) getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)).orElse(0L), currentTime);

    _consumer.stop();
  }

  /**
   * Tests that the consumer position data won't update if the consumer is unable to fetch events.
   */
  @Test
  public void testPollNoEvents() throws Exception {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _consumer = new KafkaConnectorTask(
        new KafkaBasedConnectorConfig(_factory, null, new Properties(), "", "", 1000, 5, Duration.ZERO, false,
            Duration.ZERO), _task, "", new KafkaGroupIdConstructor(false, "testCluster"));
    Thread consumerThread = new Thread(_consumer, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    _state.push(new TopicPartition("cloves", 0), 7);
    _state.push(new TopicPartition("cloves", 1), 8);
    _state.setDisabledPartitions(assignments);

    _state.allowNextPoll();
    PollUtils.poll(() -> false, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    DatastreamPositionResponse response = _consumer.getPositionResponse();

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)), Optional.of(5L));
    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)), Optional.of(6L));

    _state.allowNextPoll();
    PollUtils.poll(() -> false, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    long currentTime = System.currentTimeMillis();
    _consumer._kafkaPositionTracker.get().updateLatestBrokerOffsetsByRpc(_factory.createConsumer(null),
        _state.getAllTopicPartitions(), currentTime);
    response = _consumer.getPositionResponse();

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)), Optional.of(5L));
    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)), Optional.of(6L));

    _consumer.stop();
  }

  /**
   * Tests that the consumer position data won't update if the consumer is non-functional (polls throw exceptions and
   * consumer metric data is inaccurate).
   */
  @Test
  public void testPollWithException() throws Exception {
    Set<TopicPartition> assignments =
        new HashSet<>(Arrays.asList(new TopicPartition("cloves", 0), new TopicPartition("cloves", 1)));
    _state = new KafkaConsumerState(assignments);

    _consumer = new KafkaConnectorTask(
        new KafkaBasedConnectorConfig(_factory, null, new Properties(), "", "", 1000, 5, Duration.ZERO, false,
            Duration.ZERO), _task, "", new KafkaGroupIdConstructor(false, "testCluster"));
    Thread consumerThread = new Thread(_consumer, "Consumer Thread");
    consumerThread.setDaemon(true);
    consumerThread.start();

    _state.push(new TopicPartition("cloves", 0), 5);
    _state.push(new TopicPartition("cloves", 1), 6);

    _state.allowNextPoll();
    PollUtils.poll(() -> _producer.getEvents().size() == 2, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    _state.push(new TopicPartition("cloves", 0), 7);
    _state.push(new TopicPartition("cloves", 1), 8);
    _state.makeFaulty();

    _state.allowNextPoll();
    PollUtils.poll(() -> false, POLL_PERIOD_MS, POLL_TIMEOUT_MS);
    DatastreamPositionResponse response = _consumer.getPositionResponse();

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)), Optional.of(5L));
    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)), Optional.of(6L));

    _state.allowNextPoll();
    PollUtils.poll(() -> false, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    long currentTime = System.currentTimeMillis();
    _consumer._kafkaPositionTracker.get().updateLatestBrokerOffsetsByRpc(_factory.createConsumer(null),
        _state.getAllTopicPartitions(), currentTime);
    response = _consumer.getPositionResponse();

    _state.allowNextPoll();
    PollUtils.poll(() -> false, POLL_PERIOD_MS, POLL_TIMEOUT_MS);

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 0)), Optional.of(5L));

    Assert.assertEquals(getConsumerPositionFromPositionResponse(response, "ChicagoStylePizza",
        new TopicPartition("cloves", 1)), Optional.of(6L));

    _consumer.stop();
  }

  /**
   * Gets the current consumer position from a DatastreamPositionResponse for the specified datastream and
   * TopicPartition, if available.
   * @param response the DatastreamPositionResponse
   * @param datastream the specified datastream
   * @param topicPartition the specified TopicPartition
   * @return the current consumer position for the specified data, if available
   */
  static Optional<Long> getConsumerPositionFromPositionResponse(DatastreamPositionResponse response, String datastream,
      TopicPartition topicPartition) {
    return Optional.ofNullable(response)
        .map(DatastreamPositionResponse::getDatastreamToPhysicalSources)
        .map(datastreamToSources -> datastreamToSources.get(datastream))
        .map(sourcesToPosition -> sourcesToPosition.get(topicPartition.toString()))
        .map(PhysicalSourcePosition::getConsumerPosition)
        .map(Long::valueOf);
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
     * Returns true if the consumer state has been made faulty.
     * @return true if the consumer state has been made faulty
     */
    boolean isFaulty() {
      return _faulty;
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