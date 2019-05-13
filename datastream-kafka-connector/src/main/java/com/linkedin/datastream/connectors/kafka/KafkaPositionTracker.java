/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;

import com.linkedin.datastream.common.DurableScheduledService;
import com.linkedin.datastream.common.diag.BrooklinInstanceInfo;
import com.linkedin.datastream.common.diag.KafkaPositionKey;
import com.linkedin.datastream.common.diag.KafkaPositionValue;
import com.linkedin.datastream.common.diag.PositionDataStore;
import com.linkedin.datastream.common.diag.PositionKey;
import com.linkedin.datastream.common.diag.PositionValue;


/**
 * KafkaPositionTracker is intended to be used with a Kafka-based Connector task to keep track of the current
 * offset/position of the Connector task's consumer. This data is stored in the globally instantiated PositionDataStore.
 *
 * The information stored can then be queried via the /connectorPositions endpoint for diagnostic and analytic purposes.
 */
public class KafkaPositionTracker {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaPositionTracker.class);

  /**
   * The suffix for Kafka consumer metrics that record records lag.
   */
  private static final String RECORDS_LAG_METRIC_NAME_SUFFIX = "records-lag";

  /**
   * The frequency at which to fetch offsets from the broker using the endOffsets() RPC call.
   */
  private static final Duration BROKER_OFFSETS_FETCH_INTERVAL = Duration.ofSeconds(30);

  /**
   * The maximum duration from the last successful endOffsets() RPC call to when the Kafka consumer is assumed to be
   * faulty and need reconstructing.
   */
  private static final Duration BROKER_OFFSETS_FETCH_TIMEOUT = Duration.ofMinutes(5);

  /**
   * The number of offsets to fetch from the broker per endOffsets() RPC call. This was chosen by experiment after
   * finding timeouts with large partition counts (say 2000 or more) when using the default Kafka settings.
   */
  private static final int BROKER_OFFSETS_FETCH_SIZE = 250;

  /**
   * The task prefix for the DatastreamTask.
   * @see com.linkedin.datastream.server.DatastreamTask#getTaskPrefix()
   */
  @NotNull
  private final String _brooklinTaskPrefix;

  /**
   * The unique DatastreamTask name.
   * @see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()
   */
  @NotNull
  private final String _brooklinTaskId;

  /**
   * The time at which the Connector task was instantiated.
   */
  @NotNull
  private final Instant _taskStartTime;

  /**
   * The position data for this DatastreamTask as held by the {@link PositionDataStore}.
   */
  @NotNull
  private final ConcurrentHashMap<PositionKey, PositionValue> _positions;

  /**
   * A map of TopicPartitions to KafkaPositionKeys currently owned/operated on by this KafkaPositionTracker instance.
   */
  @NotNull
  private final ConcurrentHashMap<TopicPartition, KafkaPositionKey> _ownedKeys = new ConcurrentHashMap<>();

  /**
   * A set of TopicPartitions which are assigned to us, but for which we have not yet received any records or
   * consumer position data for.
   */
  @NotNull
  private final Set<TopicPartition> _needingInit = ConcurrentHashMap.newKeySet();

  /**
   * A LUT of TopicPartition -> MetricName as they are encountered to speed consumer metric look up.
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
   * The service responsible for periodically fetching offsets from the broker.
   */
  @NotNull
  private final DurableScheduledService _brokerOffsetsFetcher; // Defined to help investigation issues (when you have a
  // heap dump or are in a debugger)

  /**
   * Describes the metrics format supported by a Kafka consumer.
   */
  private enum ConsumerMetricsFormatSupport {
    /**
     * The Kafka consumer has no format that exposes record lag.
     */
    NONE,

    /**
     * The Kafka consumer exposes record lag by KIP-92, which applies to Kafka versions >= 0.10.2.0 and < 1.1.0.
     */
    KIP_92,

    /**
     * The Kafka consumer exposes record lag by KIP-225 (superseding KIP-92), which applies to Kafka versions >= 1.1.0.
     */
    KIP_225
  }

  /**
   * Constructor for a KafkaPositionTracker.
   *
   * @param brooklinConnectorName The name of the Connector for the given DatastreamTask
   *                             {@see com.linkedin.datastream.server.DatastreamTask#getConnectorName()}
   * @param brooklinTaskPrefix The task prefix for the DatastreamTask
   *                             {@see com.linkedin.datastream.server.DatastreamTask#getTaskPrefix()}
   * @param brooklinTaskId The DatastreamTask name
   *                       {@see com.linkedin.datastream.server.DatastreamTask#getDatastreamTaskName()}
   * @param taskStartTime The time at which the associated DatastreamTask started
   * @param isConnectorTaskAlive A Supplier that determines if the Connector task which this tracker is for is alive. If
   *                             it is not, then we should stop.
   * @param consumerSupplier A Consumer supplier that is suitable for querying the brokers that the Connector task is
   *                         talking to
   */
  public KafkaPositionTracker(@NotNull final String brooklinConnectorName, @NotNull final String brooklinTaskPrefix,
      @NotNull final String brooklinTaskId, @NotNull final Instant taskStartTime,
      @NotNull final Supplier<Boolean> isConnectorTaskAlive, @NotNull final Supplier<Consumer<?, ?>> consumerSupplier) {
    _brooklinTaskPrefix = brooklinTaskPrefix;
    _brooklinTaskId = brooklinTaskId;
    _taskStartTime = taskStartTime;
    _positions = PositionDataStore.getInstance().computeIfAbsent(brooklinConnectorName, s -> new ConcurrentHashMap<>());
    _brokerOffsetsFetcher = new DurableScheduledService(brooklinTaskId, BROKER_OFFSETS_FETCH_INTERVAL,
        BROKER_OFFSETS_FETCH_TIMEOUT) {
      private Consumer<?, ?> _consumer;

      @Override
      protected void startUp() {
        _consumer = consumerSupplier.get();
      }

      @Override
      protected void runOneIteration() {
        // Find which partitions have stale broker offset information
        final Instant staleBy = Instant.now().minus(BROKER_OFFSETS_FETCH_INTERVAL);
        final Set<TopicPartition> partitionsNeedingUpdate = new HashSet<>();
        _ownedKeys.forEach(((topicPartition, key) -> {
          // Race condition could exist where we might be unassigned the topic in a different thread while we are in
          // this thread, so do not create/initialize the value in the map.
          @Nullable final KafkaPositionValue value = (KafkaPositionValue) _positions.get(key);
          if (value != null
              && (value.getLastBrokerQueriedTime() == null || value.getLastBrokerQueriedTime().isBefore(staleBy))) {
            partitionsNeedingUpdate.add(topicPartition);
          }
        }));

        // Query the broker for its offsets for those partitions
        try {
          queryBrokerForLatestOffsets(_consumer, partitionsNeedingUpdate);
        } catch (Exception e) {
          LOG.warn("Failed to query latest broker offsets via endOffsets() RPC", e);
          throw e;
        }
      }

      @Override
      protected void signalShutdown(@Nullable final Thread taskThread) throws Exception {
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

      @Override
      protected void shutDown() {
        if (_consumer != null) {
          _consumer.close();
        }
      }

      @Override
      protected boolean hasLeaked() {
        final boolean hasLeaked = !isConnectorTaskAlive.get();
        if (hasLeaked) {
          onPartitionsRevoked(_ownedKeys.keySet());
        }
        return hasLeaked;
      }
    };
    _brokerOffsetsFetcher.startAsync();
  }

  /**
   * Initializes position data for the assigned partitions. This method should be called whenever the Connector's
   * consumer finishes assigning partitions.
   *
   * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsAssigned(Collection)
   * @param topicPartitions the topic partitions which have been assigned
   */
  public synchronized void onPartitionsAssigned(@NotNull final Collection<TopicPartition> topicPartitions) {
    final Instant assignmentTime = Instant.now();
    for (final TopicPartition topicPartition : topicPartitions) {
      final KafkaPositionKey key = new KafkaPositionKey(topicPartition.topic(), topicPartition.partition(),
          BrooklinInstanceInfo.getInstanceName(), _brooklinTaskPrefix, _brooklinTaskId, _taskStartTime);
      _ownedKeys.put(topicPartition, key);
      _needingInit.add(topicPartition);
      final KafkaPositionValue value = (KafkaPositionValue) _positions.computeIfAbsent(key, s ->
          new KafkaPositionValue());
      value.setAssignmentTime(assignmentTime);
    }
  }

  /**
   * Frees position data for partitions which have been unassigned. This method should be called whenever the
   * Connector's consumer is about to rebalance (and thus unassign partitions).
   *
   * @see org.apache.kafka.clients.consumer.ConsumerRebalanceListener#onPartitionsRevoked(Collection)
   * @param topicPartitions the topic partitions which were previously assigned
   */
  public synchronized void onPartitionsRevoked(@NotNull final Collection<TopicPartition> topicPartitions) {
    for (final TopicPartition topicPartition : topicPartitions) {
      _metricNameCache.remove(topicPartition);
      _needingInit.remove(topicPartition);
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
  public synchronized void onRecordsReceived(@NotNull final ConsumerRecords<?, ?> records,
      @NotNull final Map<MetricName, ? extends Metric> metrics) {
    final Instant receivedTime = Instant.now();
    for (final TopicPartition topicPartition : records.partitions()) {
      // It shouldn't be possible to have the key/value missing here, because we to have onPartitionsAssigned() called
      // with this topicPartition before then, but it should be safe to construct them here as this data should be
      // coming from the consumer thread without race conditions.
      final KafkaPositionKey key = _ownedKeys.computeIfAbsent(topicPartition,
          s -> new KafkaPositionKey(topicPartition.topic(), topicPartition.partition(),
              BrooklinInstanceInfo.getInstanceName(), _brooklinTaskPrefix, _brooklinTaskId, _taskStartTime));
      final KafkaPositionValue value = (KafkaPositionValue) _positions.computeIfAbsent(key,
          s -> new KafkaPositionValue());

      // Derive the consumer offset and the last record polled timestamp from the records
      long consumerOffset = Long.MIN_VALUE;
      @Nullable Instant lastRecordPolledTimestamp = null;

      for (final ConsumerRecord<?, ?> record : records.records(topicPartition)) {
        long recordOffset = record.offset();
        if (consumerOffset <= recordOffset) {
          // Why add +1? The consumer's position is the offset of the next record it expects.
          consumerOffset = recordOffset + 1;
          lastRecordPolledTimestamp = Instant.ofEpochMilli(record.timestamp());
        }
      }

      value.setLastNonEmptyPollTime(receivedTime);
      value.setConsumerOffset(consumerOffset);
      value.setLastRecordReceivedTimestamp(lastRecordPolledTimestamp);

      // Derive the broker offset from the metrics data
      @Nullable Long consumerLag = getLagMetric(metrics, topicPartition);
      if (consumerLag != null) {
        final long brokerOffset = consumerOffset + consumerLag;
        value.setLastBrokerQueriedTime(receivedTime);
        value.setBrokerOffset(brokerOffset);
      }

      // If we received records for this TopicPartition, then it won't need to be initialized
      if (!_needingInit.isEmpty()) {
        _needingInit.remove(topicPartition);
      }
    }
  }

  /**
   * Checks the Kafka consumer metrics as acquired by {@link Consumer#metrics()} to see if it contains information on
   * record lag (the lag between the consumer and the broker) for a given TopicPartition.
   *
   * If it does, the lag value is returned. Otherwise, null is returned.
   *
   * @param metrics The metrics returned by the Kafka consumer to check
   * @param topicPartition The TopicPartition to match against
   * @return the lag value if it can be found, otherwise null
   */
  @Nullable
  private Long getLagMetric(@NotNull final Map<MetricName, ? extends Metric> metrics,
      @NotNull final TopicPartition topicPartition) {
    // Check if we need to detect metrics supported by this consumer
    if (_consumerMetricsSupport == null) {
      detectConsumerMetricsSupportAndClientId(metrics);
    }

    // Terminate early if metrics are not supported by this consumer
    if (_consumerMetricsSupport == ConsumerMetricsFormatSupport.NONE) {
      return null;
    }

    // See if we already have the metric name in our cache
    @Nullable MetricName metricName = _metricNameCache.get(topicPartition);

    // Handle cache miss
    if (metricName == null) {
      if (_clientId == null) {
        detectClientId(metrics);
      }
      switch (_consumerMetricsSupport) {
        case KIP_92: {
          Map<String, String> tags = new HashMap<>();
          tags.put("client-id", _clientId);
          metricName = new MetricName(topicPartition + ".records-lag", "consumer-fetch-manager-metrics", "", tags);
          break;
        }
        case KIP_225: {
          Map<String, String> tags = new HashMap<>();
          tags.put("client-id", _clientId);
          tags.put("topic", topicPartition.topic());
          tags.put("partition", String.valueOf(topicPartition.partition()));
          metricName = new MetricName("records-lag", "consumer-fetch-manager-metrics", "", tags);
          break;
        }
        default: {
          // Client metric support is unimplemented
          return null;
        }
      }
    }
    _metricNameCache.put(topicPartition, metricName);

    @Nullable final Metric metric = metrics.get(metricName);
    if (metric != null) {
      @Nullable final Object metricValue = metric.metricValue();
      if (metricValue != null) {
        try {
          final Double doubleValue = (Double) metricValue;
          return doubleValue.longValue();
        } catch (Exception ignored) {
          LOG.debug("Metric value {} implements class {} and could not be serialized into a long", metricValue,
              metricValue.getClass());
        }
      }
    }

    return null;
  }

  /**
   * Examines the metrics returned by the Connector task's Kafka consumer to determine what metrics format type we
   * support, as well as what the Kafka consumer's client id is.
   *
   * @param metrics the metrics returned by the Connector task's Kafka consumer
   */
  private void detectConsumerMetricsSupportAndClientId(@NotNull final Map<MetricName, ? extends Metric> metrics) {
    _clientId = null;
    _consumerMetricsSupport = null;
    for (final MetricName metricName : metrics.keySet()) {
      if (metricName.name().endsWith(RECORDS_LAG_METRIC_NAME_SUFFIX)) {
        _clientId = metricName.tags().get("client-id");
        if (metricName.name().length() == RECORDS_LAG_METRIC_NAME_SUFFIX.length()) {
          _consumerMetricsSupport = ConsumerMetricsFormatSupport.KIP_225;
        } else {
          _consumerMetricsSupport = ConsumerMetricsFormatSupport.KIP_92;
        }
        return;
      }
    }
    _consumerMetricsSupport = ConsumerMetricsFormatSupport.NONE;
  }

  /**
   * Examines the metrics returned by the Connector task's Kafka consumer to determine what the Kafka consumer's client
   * id is.
   *
   * @param metrics the metrics returned by the Connector task's Kafka consumer
   */
  private void detectClientId(@NotNull final Map<MetricName, ? extends Metric> metrics) {
    if (_consumerMetricsSupport != null) {
      switch (_consumerMetricsSupport) {
        case KIP_92:
        case KIP_225:
          for (final MetricName metricName : metrics.keySet()) {
            if (metricName.name().endsWith(RECORDS_LAG_METRIC_NAME_SUFFIX)) {
              _clientId = metricName.tags().get("client-id");
              return;
            }
          }
        default: break;
      }
    }
    _clientId = null;
  }

  /**
   * Returns a Set of TopicPartitions which are assigned to us, but for which we have not yet received any records or
   * consumer position data for.
   */
  @NotNull
  public synchronized Set<TopicPartition> getPartitionsNeedingInit() {
    return _needingInit;
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
  public synchronized void initializePartition(@Nullable final TopicPartition topicPartition,
      @Nullable final Long consumerOffset) {
    if (topicPartition != null && consumerOffset != null) {
      // It shouldn't be possible to have the key/value missing here, because we to have onPartitionsAssigned() called
      // with this topicPartition before then, but it should be safe to construct them here as this data should be
      // coming from the consumer thread without race conditions.
      final KafkaPositionKey key = _ownedKeys.computeIfAbsent(topicPartition,
          s -> new KafkaPositionKey(topicPartition.topic(), topicPartition.partition(),
              BrooklinInstanceInfo.getInstanceName(), _brooklinTaskPrefix, _brooklinTaskId, _taskStartTime));
      final KafkaPositionValue value = (KafkaPositionValue) _positions.computeIfAbsent(key,
          s -> new KafkaPositionValue());
      value.setConsumerOffset(consumerOffset);
      _needingInit.remove(topicPartition);
    }
  }

  /**
   * Uses the specified consumer to make RPC calls of {@link Consumer#endOffsets(Collection)} to get the broker's latest
   * offsets for the specified TopicPartitions, and then updates the position data. The partitions will be fetched in
   * batches of {@link KafkaPositionTracker#BROKER_OFFSETS_FETCH_SIZE} to reduce the likelihood of a given call timing
   * out.
   *
   * Note that the provided consumer must not be operated on by any other thread, or a concurrent modification condition
   * may arise.
   *
   * Use externally for testing purposes only.
   */
  @VisibleForTesting
  void queryBrokerForLatestOffsets(@NotNull final Consumer<?, ?> consumer,
      @NotNull final Set<TopicPartition> partitions) {
    for (final List<TopicPartition> batch : Iterables.partition(partitions, BROKER_OFFSETS_FETCH_SIZE)) {
      final Instant queryTime = Instant.now();
      final Map<TopicPartition, Long> offsets = consumer.endOffsets(batch);
      offsets.forEach((topicPartition, offset) -> {
        if (offset != null) {
          // Race condition could exist where we might be unassigned the topic in a different thread while we are in
          // this thread, so do not create/initialize the key/value in the map.
          final KafkaPositionKey key = _ownedKeys.get(topicPartition);
          if (key != null) {
            final KafkaPositionValue value = (KafkaPositionValue) _positions.get(key);
            if (value != null) {
              value.setLastBrokerQueriedTime(queryTime);
              value.setBrokerOffset(offset);
            }
          }
        }
      });
    }
  }
}