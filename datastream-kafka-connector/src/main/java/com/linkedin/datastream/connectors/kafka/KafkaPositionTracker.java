/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.io.Closeable;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import com.linkedin.datastream.common.DurableScheduledService;
import com.linkedin.datastream.common.diag.KafkaPositionKey;
import com.linkedin.datastream.common.diag.KafkaPositionValue;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * KafkaPositionTracker is intended to be used with a Kafka-based Connector task to keep track of the current
 * offset/position of the Connector task's consumer.
 *
 * The information stored can then be queried via the /diag endpoint for diagnostic and analytic purposes.
 */
public class KafkaPositionTracker implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPositionTracker.class);

  /**
   * The suffix for Kafka consumer metrics that record records lag.
   */
  private static final String RECORDS_LAG_METRIC_NAME_SUFFIX = "records-lag";

  /**
   * The number of offsets to fetch from the broker per endOffsets() RPC call. This should be chosen to avoid timeouts
   * (larger requests are more likely to cause timeouts).
   */
  private final int _brokerOffsetsFetchSize;

  /**
   * The task prefix for the DatastreamTask.
   * @see com.linkedin.datastream.server.DatastreamTask#getTaskPrefix()
   */
  @NotNull
  private final String _datastreamTaskPrefix;

  /**
   * The unique DatastreamTask name.
   * @see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()
   */
  @NotNull
  private final String _datastreamTaskName;

  /**
   * The time at which the Connector task was instantiated.
   */
  @NotNull
  private final Instant _connectorTaskStartTime;

  /**
   * The position data for this DatastreamTask.
   */
  @NotNull
  private final ConcurrentMap<KafkaPositionKey, KafkaPositionValue> _positions = new ConcurrentHashMap<>();

  /**
   * A map of TopicPartitions to KafkaPositionKeys currently owned/operated on by this KafkaPositionTracker instance.
   */
  @NotNull
  private final ConcurrentMap<TopicPartition, KafkaPositionKey> _ownedKeys = new ConcurrentHashMap<>();

  /**
   * A set of TopicPartitions which are assigned to us, but for which we have not yet received any records or
   * consumer position data for.
   */
  @NotNull
  private final Set<TopicPartition> _uninitializedPartitions = ConcurrentHashMap.newKeySet();

  /**
   * A look-up table of TopicPartition -> MetricName as they are encountered to speed consumer metric look up.
   */
  @NotNull
  private final Map<TopicPartition, MetricName> _metricNameCache = new HashMap<>();

  /**
   * The client id of the Kafka consumer used by the Connector task. Used to fetch metrics.
   */
  @Nullable
  private String _clientId;

  /**
   * The metrics format supported by the Kafka consumer used by the Connector task.
   */
  @Nullable
  private ConsumerMetricsFormatSupport _consumerMetricsSupport;

  /**
   * A Supplier that determines if the Connector task which this tracker is for is alive. If it is not, then we should
   * stop running.
   */
  private final Supplier<Consumer<?, ?>> _consumerSupplier;

  /**
   * The service responsible for periodically fetching offsets from the broker.
   */
  @Nullable
  private final BrokerOffsetFetcher _brokerOffsetFetcher; // Defined to help investigation issues (when you have a
  // heap dump or are in a debugger)

  /**
   * Describes the metrics format supported by a Kafka consumer.
   */
  private enum ConsumerMetricsFormatSupport {
    /**
     * The Kafka consumer exposes record lag by KIP-92, which applies to Kafka versions >= 0.10.2.0 and < 1.1.0.
     * @see <a href="https://cwiki.apache.org/confluence/x/bhX8Awr">KIP-92</a>
     */
    KIP_92,

    /**
     * The Kafka consumer exposes record lag by KIP-225 (superseding KIP-92), which applies to Kafka versions >= 1.1.0.
     * @see <a href="https://cwiki.apache.org/confluence/x/uaBzB">KIP-225</a>
     */
    KIP_225
  }

  /**
   * Constructor for a KafkaPositionTracker.
   *
   * @param datastreamTaskPrefix The task prefix for the DatastreamTask
   *                             {@see com.linkedin.datastream.server.DatastreamTask#getTaskPrefix()}
   * @param datastreamTaskName The DatastreamTask name
   *                           {@see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()}
   * @param connectorTaskStartTime The time at which the associated DatastreamTask started
   * @param isConnectorTaskAlive A Supplier that determines if the Connector task which this tracker is for is alive. If
   *                             it is not, then we should stop.
   * @param consumerSupplier A Consumer supplier that is suitable for querying the brokers that the Connector task is
   *                         talking to
   * @param positionTrackerConfig User-supplied configuration settings
   */
  private KafkaPositionTracker(@NotNull String datastreamTaskPrefix, @NotNull String datastreamTaskName,
      @NotNull Instant connectorTaskStartTime, @NotNull Supplier<Boolean> isConnectorTaskAlive,
      @NotNull Supplier<Consumer<?, ?>> consumerSupplier, KafkaPositionTrackerConfig positionTrackerConfig) {
    _datastreamTaskPrefix = datastreamTaskPrefix;
    _datastreamTaskName = datastreamTaskName;
    _connectorTaskStartTime = connectorTaskStartTime;
    _consumerSupplier = consumerSupplier;
    _brokerOffsetsFetchSize = positionTrackerConfig.getBrokerOffsetsFetchSize();
    if (positionTrackerConfig.isEnableBrokerOffsetFetcher()) {
      _brokerOffsetFetcher = new BrokerOffsetFetcher(datastreamTaskName, this, isConnectorTaskAlive,
          positionTrackerConfig);
      _brokerOffsetFetcher.startAsync();
    } else {
      _brokerOffsetFetcher = null;
    }
  }

  /**
   * Initializes position data for the assigned partitions. This method should be called whenever the Connector's
   * consumer finishes assigning partitions.
   *
   * @see AbstractKafkaBasedConnectorTask#onPartitionsAssigned(Collection) for how this method is used in a connector
   *      task
   * @param topicPartitions the topic partitions which have been assigned
   */
  public synchronized void onPartitionsAssigned(@NotNull Collection<TopicPartition> topicPartitions) {
    final Instant assignmentTime = Instant.now();
    for (final TopicPartition topicPartition : topicPartitions) {
      final KafkaPositionKey key = new KafkaPositionKey(topicPartition.topic(), topicPartition.partition(),
          _datastreamTaskPrefix, _datastreamTaskName, _connectorTaskStartTime);
      _ownedKeys.put(topicPartition, key);
      _uninitializedPartitions.add(topicPartition);
      final KafkaPositionValue value = _positions.computeIfAbsent(key, s -> new KafkaPositionValue());
      value.setAssignmentTime(assignmentTime);
    }
  }

  /**
   * Frees position data for partitions which have been unassigned. This method should be called whenever the
   * Connector's consumer is about to rebalance (and thus unassign partitions).
   *
   * @see AbstractKafkaBasedConnectorTask#onPartitionsRevoked(Collection) for how this method is used in a connector
   *      task
   * @param topicPartitions the topic partitions which were previously assigned
   */
  public synchronized void onPartitionsRevoked(@NotNull Collection<TopicPartition> topicPartitions) {
    for (final TopicPartition topicPartition : topicPartitions) {
      _metricNameCache.remove(topicPartition);
      _uninitializedPartitions.remove(topicPartition);
      @Nullable final KafkaPositionKey key = _ownedKeys.remove(topicPartition);
      if (key != null) {
        _positions.remove(key);
      }
    }
  }

  /**
   * Updates the position data after the Connector's consumer has finished polling, using both the returned records and
   * the available internal Kafka consumer metrics.
   *
   * This method will only update position data for partitions which have received records.
   *
   * @param records the records fetched from {@link Consumer#poll(Duration)}
   * @param metrics the metrics for the Kafka consumer as fetched from {@link Consumer#metrics()}
   */
  public synchronized void onRecordsReceived(@NotNull ConsumerRecords<?, ?> records,
      @NotNull Map<MetricName, ? extends Metric> metrics) {
    final Instant receivedTime = Instant.now();
    for (final TopicPartition topicPartition : records.partitions()) {
      // It shouldn't be possible to have the key/value missing here, because we to have onPartitionsAssigned() called
      // with this topicPartition before then, but it should be safe to construct them here as this data should be
      // coming from the consumer thread without race conditions.
      final KafkaPositionKey key = _ownedKeys.computeIfAbsent(topicPartition,
          s -> new KafkaPositionKey(topicPartition.topic(), topicPartition.partition(), _datastreamTaskPrefix,
              _datastreamTaskName, _connectorTaskStartTime));
      final KafkaPositionValue value = _positions.computeIfAbsent(key, s -> new KafkaPositionValue());

      // Derive the consumer offset and the last record polled timestamp from the records
      records.records(topicPartition).stream()
          .max(Comparator.comparingLong(ConsumerRecord::offset))
          .ifPresent(record -> {
            value.setLastNonEmptyPollTime(receivedTime);
            // Why add +1? The consumer's position is the offset of the next record it expects.
            value.setConsumerOffset(record.offset() + 1);
            value.setLastRecordReceivedTimestamp(Instant.ofEpochMilli(record.timestamp()));
          });

      // Attempt derive the broker's offset from the consumer's metrics
      getLagMetric(metrics, topicPartition).ifPresent(consumerLag ->
          Optional.ofNullable(value.getConsumerOffset()).ifPresent(consumerOffset -> {
            // If we know both the consumer's lag from the metrics, and the consumer's offset from our position data,
            // then we can calculate what the broker's offset should be.
            final long brokerOffset = consumerOffset + consumerLag;
            value.setLastBrokerQueriedTime(receivedTime);
            value.setBrokerOffset(brokerOffset);
          }));

      _uninitializedPartitions.remove(topicPartition);
    }
  }

  /**
   * Checks the Kafka consumer metrics as acquired by {@link Consumer#metrics()} to see if it contains information on
   * record lag (the lag between the consumer and the broker) for a given TopicPartition.
   *
   * If it does, the lag value is returned.
   *
   * @param metrics The metrics returned by the Kafka consumer to check
   * @param topicPartition The TopicPartition to match against
   * @return the lag value if it can be found
   */
  @NotNull
  private Optional<Long> getLagMetric(@NotNull Map<MetricName, ? extends Metric> metrics,
      @NotNull TopicPartition topicPartition) {
    @Nullable final MetricName metricName = Optional.ofNullable(_metricNameCache.get(topicPartition))
        .orElseGet(() -> tryCreateMetricName(topicPartition, metrics.keySet()).orElse(null));
    return Optional.ofNullable(metricName)
        .map(name -> (Metric) metrics.get(name))
        .map(Metric::metricValue)
        .filter(value -> value instanceof Double)
        .map(value -> ((Double) value).longValue());
  }

  /**
   * Attempts to return the metric name containing record lag information if it exists. This method will attempt to
   * return the cached value before calculating it (calculating the value is expensive).
   *
   * @param topicPartition the provided topic partition
   * @param metricNames the collection of metric names
   * @return the metric name containing record lag information, if it can be derived
   */
  @NotNull
  private Optional<MetricName> tryCreateMetricName(@NotNull TopicPartition topicPartition,
      @NotNull Collection<MetricName> metricNames) {
    // Try to fetch the result from cache first
    MetricName metricName = _metricNameCache.get(topicPartition);
    if (metricName != null) {
      return Optional.of(metricName);
    }

    // Try to initialize the variables if they are not set
    if (_clientId == null || _consumerMetricsSupport == null) {
      // Find a testable metric name in the collection
      metricNames.stream()
          .filter(candidateMetricName -> candidateMetricName.name() != null)
          .filter(candidateMetricName -> candidateMetricName.tags() != null)
          .filter(candidateMetricName -> candidateMetricName.name().endsWith(RECORDS_LAG_METRIC_NAME_SUFFIX))
          .findAny()
          .ifPresent(testableMetricName -> {
            // Attempt to extract the consumer's client id and the consumer's metric support level through the testable
            // metric name
            _clientId = Optional.ofNullable(testableMetricName.tags()).map(tags -> tags.get("client-id")).orElse(null);
            _consumerMetricsSupport = testableMetricName.name().length() == RECORDS_LAG_METRIC_NAME_SUFFIX.length()
                ? ConsumerMetricsFormatSupport.KIP_225 : ConsumerMetricsFormatSupport.KIP_92;
          });
    }

    // Ensure our variables are initialized (they should be, but we are being extra defensive)
    @Nullable final String clientId = _clientId;
    @Nullable final ConsumerMetricsFormatSupport consumerMetricsSupport = _consumerMetricsSupport;
    if (clientId == null || consumerMetricsSupport == null) {
      // Client metric support is unimplemented in the current consumer
      LOG.trace("The current consumer does not seem to have metric support for record lag.");
      return Optional.empty();
    }

    // Build our metric name
    switch (consumerMetricsSupport) {
      case KIP_92: {
        final Map<String, String> tags = new HashMap<>();
        tags.put("client-id", clientId);
        metricName = new MetricName(topicPartition + "." + RECORDS_LAG_METRIC_NAME_SUFFIX,
            "consumer-fetch-manager-metrics", "", tags);
        break;
      }
      case KIP_225: {
        final Map<String, String> tags = new HashMap<>();
        tags.put("client-id", clientId);
        tags.put("topic", topicPartition.topic());
        tags.put("partition", String.valueOf(topicPartition.partition()));
        metricName = new MetricName(RECORDS_LAG_METRIC_NAME_SUFFIX, "consumer-fetch-manager-metrics", "", tags);
        break;
      }
      default: {
        // Client metric support is unimplemented in the current consumer
        LOG.trace("The current consumer does not seem to have metric support for record lag.");
        return Optional.empty();
      }
    }

    // Store it in the cache and return it
    _metricNameCache.put(topicPartition, metricName);
    return Optional.of(metricName);
  }

  /**
   * Returns a Set of TopicPartitions which are assigned to us, but for which we have not yet received any records or
   * consumer position data for.
   *
   * @return the Set of TopicPartitions which are not yet initialized
   */
  @NotNull
  public synchronized Set<TopicPartition> getUninitializedPartitions() {
    return Collections.unmodifiableSet(_uninitializedPartitions);
  }

  /**
   * Fills the current consumer offset into the position data for the given TopicPartition, causing the TopicPartition
   * to no longer need initialization.
   *
   * This offset is typically found by the Connector's consumer by calling {@link Consumer#position(TopicPartition)}
   * after a successful {@link Consumer#poll(Duration)}.
   *
   * @param topicPartition The given TopicPartition to provide position data for
   * @param consumerOffset The Connector consumer's offset for this topic partition as if specified by
   *                       {@link Consumer#position(TopicPartition)}
   */
  public synchronized void initializePartition(@Nullable TopicPartition topicPartition, @Nullable Long consumerOffset) {
    if (topicPartition != null && consumerOffset != null) {
      // It shouldn't be possible to have the key/value missing here, because we to have onPartitionsAssigned() called
      // with this topicPartition before then, but it should be safe to construct them here as this data should be
      // coming from the consumer thread without race conditions.
      final KafkaPositionKey key = _ownedKeys.computeIfAbsent(topicPartition,
          s -> new KafkaPositionKey(topicPartition.topic(), topicPartition.partition(), _datastreamTaskPrefix,
              _datastreamTaskName, _connectorTaskStartTime));
      final KafkaPositionValue value = _positions.computeIfAbsent(key, s -> new KafkaPositionValue());
      value.setConsumerOffset(consumerOffset);
      _uninitializedPartitions.remove(topicPartition);
    }
  }

  /**
   * Returns the position data stored in this instance.
   *
   * @return the position data stored in this instance
   */
  public Map<KafkaPositionKey, KafkaPositionValue> getPositions() {
    return Collections.unmodifiableMap(_positions);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void close() {
    final BrokerOffsetFetcher brokerOffsetFetcher = _brokerOffsetFetcher;
    if (brokerOffsetFetcher != null) {
      brokerOffsetFetcher.stopAsync();
    }
    onPartitionsRevoked(_ownedKeys.keySet());
  }

  /**
   * Returns a Builder which can be used to construct a {@link KafkaPositionTracker}.
   *
   * @return a Builder for this class
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * Uses the specified consumer to make RPC calls of {@link Consumer#endOffsets(Collection)} to get the broker's latest
   * offsets for the specified TopicPartitions, and then updates the position data. The partitions will be fetched in
   * batches to reduce the likelihood of a given call timing out.
   *
   * Note that the provided consumer must not be operated on by any other thread, or a concurrent modification condition
   * may arise.
   *
   * Ideally the partitions provided to this method are owned all by the same broker for performance purposes.
   *
   * Use externally for testing purposes only.
   */
  @VisibleForTesting
  void queryBrokerForLatestOffsets(@NotNull Consumer<?, ?> consumer, @NotNull Set<TopicPartition> partitions,
      @NotNull Duration requestTimeout) {
    for (final List<TopicPartition> batch : Iterables.partition(partitions, _brokerOffsetsFetchSize)) {
      LOG.trace("Fetching the latest offsets for partitions: {}", batch);
      final Instant queryTime = Instant.now();
      final Map<TopicPartition, Long> offsets;
      try {
        offsets = consumer.endOffsets(batch, requestTimeout);
      } catch (Exception e) {
        LOG.trace("Unable to fetch latest offsets for partitions: {}", batch, e);
        continue;
      } finally {
        LOG.trace("Finished fetching the latest offsets for partitions {} in {} ms", batch,
            Duration.between(queryTime, Instant.now()).toMillis());
      }
      offsets.forEach((topicPartition, offset) -> {
        if (offset != null) {
          // Race condition could exist where we might be unassigned the topic in a different thread while we are in
          // this thread, so do not create/initialize the key/value in the map.
          final KafkaPositionKey key = _ownedKeys.get(topicPartition);
          if (key != null) {
            final KafkaPositionValue value = _positions.get(key);
            if (value != null) {
              value.setLastBrokerQueriedTime(queryTime);
              value.setBrokerOffset(offset);
            }
          }
        }
      });
    }
  }

  /**
   * Supplies a consumer usable for fetching broker offsets.
   *
   * @return a consumer usable for RPC calls
   */
  @VisibleForTesting
  Supplier<Consumer<?, ?>> getConsumerSupplier() {
    return _consumerSupplier;
  }

  /**
   * Implements a periodic service which queries the broker for its latest partition offsets.
   */
  private static class BrokerOffsetFetcher extends DurableScheduledService {
    /**
     * The frequency at which to fetch offsets from the broker using the endOffsets() RPC call.
     */
    private final Duration _brokerOffsetsFetchInterval;

    /**
     * The maximum duration that a consumer request is allowed to take before it times out.
     */
    private final Duration _consumerRequestTimeout;

    /**
     * The KafkaPositionTracker object which created us.
     */
    private final KafkaPositionTracker _kafkaPositionTracker;

    /**
     * A Consumer supplier that is suitable for querying the brokers that the Connector task is talking to.
     */
    private final Supplier<Boolean> _isConnectorTaskAlive;

    /**
     * True if we should calculate performance leadership to improve the broker query performance, false otherwise.
     */
    private final boolean _enablePartitionLeadershipCalculation;

    /**
     * The frequency at which we should fetch partition leadership.
     */
    private final Duration _partitionLeadershipCalculationFrequency;

    /**
     * A cache of partition leadership used to optimize the endOffset() RPC calls made. This is necessary as an
     * endOffset() RPC call can in a very large fanout of requests (as it may need to talk to a lot of brokers). By
     * keeping a very weakly consistent list of who owns what partitions, we can offer a performance boost. If this map
     * is very out-of-date, it is unlikely to be much worse than not using it at all.
     */
    private final Map<TopicPartition, Node> _partitionLeadershipMap = new ConcurrentHashMap<>();

    /**
     * The underlying Consumer used to make the endOffsets() RPC call.
     */
    private Consumer<?, ?> _consumer;

    /**
     * The time of the last partition leadership calculation.
     */
    private Instant _lastPartitionLeadershipCalculation;

    /**
     * Constructor for this class.
     *
     * @param brooklinTaskId The DatastreamTask name
     *                       {@see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()}
     * @param kafkaPositionTracker The KafkaPositionTracker instantiating this object
     * @param isConnectorTaskAlive A Supplier that determines if the Connector task which this tracker is for is alive.
     *                             If it is not, then we should stop
     * @param positionTrackerConfig User-supplied configuration settings
     */
    public BrokerOffsetFetcher(@NotNull String brooklinTaskId, @NotNull KafkaPositionTracker kafkaPositionTracker,
        @NotNull Supplier<Boolean> isConnectorTaskAlive, @NotNull KafkaPositionTrackerConfig positionTrackerConfig) {
      super("KafkaPositionTracker-" + brooklinTaskId, positionTrackerConfig.getBrokerOffsetFetcherInterval(),
          positionTrackerConfig.getConsumerFailedDetectionThreshold());
      _kafkaPositionTracker = kafkaPositionTracker;
      _isConnectorTaskAlive = isConnectorTaskAlive;
      _enablePartitionLeadershipCalculation = positionTrackerConfig.isEnablePartitionLeadershipCalculation();
      _brokerOffsetsFetchInterval = positionTrackerConfig.getBrokerOffsetFetcherInterval();
      _consumerRequestTimeout = positionTrackerConfig.getBrokerRequestTimeout();
      _partitionLeadershipCalculationFrequency = positionTrackerConfig.getPartitionLeadershipCalculationFrequency();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void startUp() {
      _consumer = _kafkaPositionTracker._consumerSupplier.get();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void runOneIteration() {
      // Find which partitions have stale broker offset information
      final Instant staleBy = Instant.now().minus(_brokerOffsetsFetchInterval);
      final Set<TopicPartition> partitionsNeedingUpdate = new HashSet<>();
      _kafkaPositionTracker._ownedKeys.forEach(((topicPartition, key) -> {
        // Race condition could exist where we might be unassigned the topic in a different thread while we are in
        // this thread, so do not create/initialize the value in the map.
        @Nullable final KafkaPositionValue value = _kafkaPositionTracker._positions.get(key);
        if (value != null
            && (value.getLastBrokerQueriedTime() == null || value.getLastBrokerQueriedTime().isBefore(staleBy))) {
          partitionsNeedingUpdate.add(topicPartition);
        }
      }));

      try {
        if (_enablePartitionLeadershipCalculation) {
          Instant calculationStaleBy = Optional.ofNullable(_lastPartitionLeadershipCalculation)
              .map(lastCalculation -> lastCalculation.plus(_partitionLeadershipCalculationFrequency))
              .orElse(Instant.MIN);
          if (Instant.now().isAfter(calculationStaleBy)) {
            updatePartitionLeadershipMap();
            _lastPartitionLeadershipCalculation = Instant.now();
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to update the partition leadership map. Using stale leadership data. Reason: {}", e.getMessage());
        LOG.debug("Failed to update the partition leadership map. Using stale leadership data.", e);
      }

      // Query the broker for its offsets for those partitions
      batchPartitionsByLeader(partitionsNeedingUpdate).forEach(partitionLeaderBatch -> {
        try {
          _kafkaPositionTracker.queryBrokerForLatestOffsets(_consumer, partitionLeaderBatch, _consumerRequestTimeout);
        } catch (Exception e) {
          LOG.warn("Failed to query latest broker offsets for this leader batch via endOffsets() RPC. Reason: {}", e.getMessage());
          LOG.debug("Failed to query latest broker offsets for this leader batch via endOffsets() RPC", e);
        }
      });
    }

    /**
     * Queries a Kafka broker for the leader of each partition and caches that information.
     */
    private void updatePartitionLeadershipMap() {
      LOG.debug("Updating partition leadership map");
      Optional.ofNullable(_consumer.listTopics(_consumerRequestTimeout)).ifPresent(response -> {
        Map<TopicPartition, Node> updateMap = response.values().stream()
            .filter(Objects::nonNull)
            .flatMap(Collection::stream)
            .filter(partitionInfo -> partitionInfo != null && partitionInfo.topic() != null && partitionInfo.leader() != null)
            .collect(Collectors.toMap(
                partitionInfo -> new TopicPartition(partitionInfo.topic(), partitionInfo.partition()),
                PartitionInfo::leader));
        _partitionLeadershipMap.keySet().retainAll(updateMap.keySet());
        _partitionLeadershipMap.putAll(updateMap);
      });
    }

    /**
     * Groups topic partitions into batches based on their leader.
     * @param topicPartitions the topic partitions to group by leader
     * @return a list of topic partitions batches
     */
    @NotNull
    private List<Set<TopicPartition>> batchPartitionsByLeader(@NotNull Set<TopicPartition> topicPartitions) {
      if (!_enablePartitionLeadershipCalculation || _partitionLeadershipMap.isEmpty()) {
        if (_partitionLeadershipMap.isEmpty()) {
          LOG.debug("Leadership unknown for all topic partitions");
        }
        return Collections.singletonList(topicPartitions);
      }

      final Map<Node, Set<TopicPartition>> assignedPartitions = new HashMap<>();
      final Set<TopicPartition> unassignedPartitions = new HashSet<>();

      topicPartitions.forEach(topicPartition -> {
        @Nullable final Node leader = _partitionLeadershipMap.get(topicPartition);
        if (leader == null) {
          LOG.debug("Leader unknown for topic partition {}", topicPartition);
          unassignedPartitions.add(topicPartition);
        } else {
          LOG.trace("Leader for topic partition {} is {}", topicPartition, leader);
          assignedPartitions.computeIfAbsent(leader, s -> new HashSet<>()).add(topicPartition);
        }
      });

      final List<Set<TopicPartition>> batches = new ArrayList<>();
      batches.addAll(assignedPartitions.values());
      batches.add(unassignedPartitions);
      return batches;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void signalShutdown(@Nullable Thread taskThread) throws Exception {
      if (taskThread != null && taskThread.isAlive()) {
        // Attempt to gracefully interrupt the consumer
        _consumer.wakeup();

        // Wait up to ten seconds for success
        taskThread.join(Duration.ofSeconds(10).toMillis());

        if (taskThread.isAlive()) {
          // Attempt to more aggressively interrupt the consumer
          taskThread.interrupt();
        }
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void shutDown() {
      if (_consumer != null) {
        _consumer.close();
      }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean hasLeaked() {
      final boolean hasLeaked = !_isConnectorTaskAlive.get();
      if (hasLeaked) {
        _kafkaPositionTracker.onPartitionsRevoked(_kafkaPositionTracker._ownedKeys.keySet());
      }
      return hasLeaked;
    }
  }

  /**
   * Builder which can be used to create an instance of {@link KafkaPositionTracker}.
   */
  public static class Builder {
    private Instant _connectorTaskStartTime;
    private Supplier<Consumer<?, ?>> _consumerSupplier;
    private String _datastreamTaskName;
    private String _datastreamTaskPrefix;
    private Supplier<Boolean> _isConnectorTaskAlive;
    private KafkaPositionTrackerConfig _kafkaPositionTrackerConfig;

    /**
     * Configures this builder with the time at which the associated DatastreamTask was started. This value is required
     * to construct a {@link KafkaPositionTracker} object and must be provided before {@link #build()} is called.
     *
     * @param connectorTaskStartTime the time at which the associated DatastreamTask was started
     * @return a builder configured with the connectorTaskStartTime param
     */
    @NotNull
    public Builder withConnectorTaskStartTime(@NotNull Instant connectorTaskStartTime) {
      _connectorTaskStartTime = connectorTaskStartTime;
      return this;
    }

    /**
     * Configures the builder with a Consumer supplier that is suitable for querying the brokers that the Connector task
     * is talking to. This value is required to construct a {@link KafkaPositionTracker} object and must be provided
     * before {@link #build()} is called.
     *
     * @param consumerSupplier A Consumer supplier that is suitable for querying the brokers that the Connector task is
     *                         talking to
     * @return a builder configured with the consumerSupplier param
     */
    @NotNull
    public Builder withConsumerSupplier(@NotNull Supplier<Consumer<?, ?>> consumerSupplier) {
      _consumerSupplier = consumerSupplier;
      return this;
    }

    /**
     * Configures the builder with the associated {@link DatastreamTask}. This class requires two pieces of data from
     * the DatastreamTask it is associated with, the DatastreamTask's task prefix and task name, which it collects using
     * {@link DatastreamTask#getTaskPrefix()} and {@link DatastreamTask#getDatastreamTaskName()} respectively. These
     * values are required to construct a {@link KafkaPositionTracker} object and must be provided before
     * {@link #build()} is called.
     *
     * @param datastreamTask The DatastreamTask associated with this position tracker
     * @return a builder configured with the datastreamTaskPrefix and datastreamTaskName params acquired from the
     *         provided DatastreamTask
     */
    @NotNull
    public Builder withDatastreamTask(@NotNull DatastreamTask datastreamTask) {
      _datastreamTaskName = datastreamTask.getDatastreamTaskName();
      _datastreamTaskPrefix = datastreamTask.getTaskPrefix();
      return this;
    }

    /**
     * Configures the builder with a function that allows us to check if the connector task is alive. If it is not
     * alive, then we should ensure this position tracker is closed down properly. This value is required to construct a
     * {@link KafkaPositionTracker} object and must be provided before {@link #build()} is called.
     *
     * @param isConnectorTaskAlive A Supplier that determines if the Connector task which this tracker is for is alive
     * @return a builder configured with the isConnectorTaskAlive param
     */
    @NotNull
    public Builder withIsConnectorTaskAlive(@NotNull Supplier<Boolean> isConnectorTaskAlive) {
      _isConnectorTaskAlive = isConnectorTaskAlive;
      return this;
    }

    /**
     * Configures the builder with various user-supplied configuration settings.
     *
     * @param kafkaPositionTrackerConfig user-supplied configuration settings
     * @return a builder configured with the kafkaPositionTrackerConfig param
     */
    @NotNull
    public Builder withKafkaPositionTrackerConfig(@NotNull KafkaPositionTrackerConfig kafkaPositionTrackerConfig) {
      _kafkaPositionTrackerConfig = kafkaPositionTrackerConfig;
      return this;
    }

    /**
     * Uses the information already provided to the current instantiation of the builder to create an instance of the
     * {@link KafkaPositionTracker} class.
     *
     * @return a {@link KafkaPositionTracker} object configured using the instance data from this builder
     */
    @NotNull
    public KafkaPositionTracker build() {
      return new KafkaPositionTracker(_datastreamTaskPrefix, _datastreamTaskName, _connectorTaskStartTime,
          _isConnectorTaskAlive, _consumerSupplier, _kafkaPositionTrackerConfig);
    }
  }
}