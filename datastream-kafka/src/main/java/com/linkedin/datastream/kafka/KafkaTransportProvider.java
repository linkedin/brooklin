/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import org.apache.commons.lang3.Validate;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.metrics.BrooklinMeterInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamTask;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


/**
 * This is Kafka Transport provider that writes events to Kafka.
 */
public class KafkaTransportProvider implements TransportProvider {
  private static final String CLASS_NAME = KafkaTransportProvider.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(CLASS_NAME);

  static final String AGGREGATE = "aggregate";
  static final String EVENT_WRITE_RATE = "eventWriteRate";
  static final String EVENT_BYTE_WRITE_RATE = "eventByteWriteRate";
  static final String EVENT_TRANSPORT_ERROR_RATE = "eventTransportErrorRate";

  private final DatastreamTask _datastreamTask;
  private List<KafkaProducerWrapper<byte[], byte[]>> _producers;

  private final DynamicMetricsManager _dynamicMetricsManager;
  private final String _metricsNamesPrefix;
  private final Meter _eventWriteRate;
  private final Meter _eventByteWriteRate;
  private final Meter _eventTransportErrorRate;

  public KafkaTransportProvider(DatastreamTask datastreamTask, List<KafkaProducerWrapper<byte[], byte[]>> producers,
      Properties props, String metricsNamesPrefix) {
    org.apache.commons.lang.Validate.notNull(datastreamTask, "null tasks");
    org.apache.commons.lang.Validate.notNull(producers, "null producer wrappers");
    _producers = producers;
    _datastreamTask = datastreamTask;
    LOG.info("Creating kafka transport provider with properties: {}", props);
    if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      String errorMessage = "Bootstrap servers are not set";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    // initialize metrics
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _metricsNamesPrefix = metricsNamesPrefix == null ? CLASS_NAME : metricsNamesPrefix + CLASS_NAME;
    _eventWriteRate = new Meter();
    _eventByteWriteRate = new Meter();
    _eventTransportErrorRate = new Meter();
  }

  public List<KafkaProducerWrapper<byte[], byte[]>> getProducers() {
    return _producers;
  }

  private ProducerRecord<byte[], byte[]> convertToProducerRecord(String topicName,
      DatastreamProducerRecord record, Object event) throws DatastreamException {

    Optional<Integer> partition = record.getPartition();

    byte[] keyValue = new byte[0];
    byte[] payloadValue = new byte[0];
    if (event instanceof BrooklinEnvelope) {
      BrooklinEnvelope envelope = (BrooklinEnvelope) event;
      if (envelope.key().isPresent() && envelope.key().get() instanceof byte[]) {
        keyValue = (byte[]) envelope.key().get();
      }

      if (envelope.value().isPresent() && envelope.value().get() instanceof byte[]) {
        payloadValue = (byte[]) envelope.value().get();
      }
    } else if (event instanceof byte[]) {
      payloadValue = (byte[]) event;
    }

    if (partition.isPresent() && partition.get() >= 0) {
      // If the partition is specified. We send the record to the specific partition
      return new ProducerRecord<>(topicName, partition.get(), keyValue, payloadValue);
    } else {
      // If the partition is not specified. We use the partitionKey as the key. Kafka will use the hash of that
      // to determine the partition. If partitionKey does not exist, use the key value.
      keyValue = record.getPartitionKey().isPresent() ? record.getPartitionKey().get().getBytes() : keyValue;
      return new ProducerRecord<>(topicName, keyValue, payloadValue);
    }
  }

  @Override
  public void send(String destinationUri, DatastreamProducerRecord record, SendCallback onSendComplete) {
    String topicName = KafkaTransportProviderUtils.getTopicName(destinationUri);
    try {
      Validate.notNull(record, "null event record.");
      Validate.notNull(record.getEvents(), "null datastream events.");
      Validate.isTrue(record.getPartition().isPresent() || record.getPartitionKey().isPresent(),
          "Either partition or partitionKey needs to be set");

      LOG.debug("Sending Datastream event record: {}", record);

      for (Object event : record.getEvents()) {
        ProducerRecord<byte[], byte[]> outgoing;
        try {
          outgoing = convertToProducerRecord(topicName, record, event);
        } catch (Exception e) {
          String errorMessage = String.format("Failed to convert DatastreamEvent (%s) to ProducerRecord.", event);
          LOG.error(errorMessage, e);
          throw new DatastreamRuntimeException(errorMessage, e);
        }
        _eventWriteRate.mark();
        _eventByteWriteRate.mark(outgoing.key().length + outgoing.value().length);

        KafkaProducerWrapper<byte[], byte[]> producer =
            _producers.get(Math.abs(Objects.hash(outgoing.topic(), outgoing.partition())) % _producers.size());

        producer.send(_datastreamTask, outgoing, (metadata, exception) -> {
          int partition = metadata != null ? metadata.partition() : -1;
          if (exception != null) {
            LOG.error("Sending a message with source checkpoint {} to topic {} partition {} for datastream task {} "
                    + "threw an exception.", record.getCheckpoint(), topicName, partition, _datastreamTask, exception);
          }
          doOnSendCallback(record, onSendComplete, metadata, exception);
        });

        // Update topic-specific metrics and aggregate metrics
        int numBytes = outgoing.key().length + outgoing.value().length;
        _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, topicName, EVENT_WRITE_RATE,
            1);
        _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, topicName,
            EVENT_BYTE_WRITE_RATE, numBytes);
        _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, AGGREGATE, EVENT_WRITE_RATE, 1);
        _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, AGGREGATE, EVENT_BYTE_WRITE_RATE, numBytes);
      }
    } catch (Exception e) {
      _eventTransportErrorRate.mark();
      _dynamicMetricsManager.createOrUpdateMeter(_metricsNamesPrefix, topicName, EVENT_TRANSPORT_ERROR_RATE, 1);
      String errorMessage = String.format(
          "Sending DatastreamRecord (%s) to topic %s, partition %s, Kafka cluster %s failed with exception.", record,
          topicName, record.getPartition().orElse(-1), destinationUri);

      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }

    LOG.debug("Done sending Datastream event record: {}", record);
  }

  @Override
  public void close() {
    _producers.forEach(p -> p.close(_datastreamTask));
  }

  @Override
  public void flush() {
    _producers.forEach(p -> p.flush(_datastreamTask));
  }

  private void doOnSendCallback(DatastreamProducerRecord record, SendCallback onComplete, RecordMetadata metadata,
      Exception exception) {
    if (onComplete != null) {
      onComplete.onCompletion(
          metadata != null ? new DatastreamRecordMetadata(record.getCheckpoint(), metadata.topic(),
              metadata.partition()) : null, exception);
    }
  }

  public static List<BrooklinMetricInfo> getMetricInfos(String metricsNamesPrefix) {
    String prefix = metricsNamesPrefix == null ? CLASS_NAME + MetricsAware.KEY_REGEX
        : metricsNamesPrefix + CLASS_NAME + MetricsAware.KEY_REGEX;

    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.add(new BrooklinMeterInfo(prefix + EVENT_WRITE_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + EVENT_BYTE_WRITE_RATE));
    metrics.add(new BrooklinMeterInfo(prefix + EVENT_TRANSPORT_ERROR_RATE));

    return Collections.unmodifiableList(metrics);
  }
}
