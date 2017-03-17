package com.linkedin.datastream.kafka;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Meter;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.DatastreamException;
import com.linkedin.datastream.common.DatastreamRuntimeException;
import com.linkedin.datastream.common.ErrorLogger;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.api.transport.DatastreamRecordMetadata;
import com.linkedin.datastream.server.api.transport.SendCallback;
import com.linkedin.datastream.server.api.transport.TransportProvider;


/**
 * This is Kafka Transport provider that writes events to kafka.
 */
public class KafkaTransportProvider implements TransportProvider {
  private static final String MODULE = KafkaTransportProvider.class.getSimpleName();
  private static final Logger LOG = LoggerFactory.getLogger(MODULE);

  private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private static final String VAL_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

  private final KafkaProducer<byte[], byte[]> _producer;
  private final String _brokers;

  private static final String EVENT_WRITE_RATE = "eventWriteRate";
  private static final String EVENT_BYTE_WRITE_RATE = "eventByteWriteRate";
  private static final String EVENT_TRANSPORT_ERROR_RATE = "eventTransportErrorRate";

  private final DynamicMetricsManager _dynamicMetricsManager;
  private final Meter _eventWriteRate;
  private final Meter _eventByteWriteRate;
  private final Meter _eventTransportErrorRate;
  private static final String AGGREGATE = "aggregate";

  public KafkaTransportProvider(Properties props) {
    LOG.info(String.format("Creating kafka transport provider with properties: %s", props));
    if (!props.containsKey(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
      String errorMessage = "Bootstrap servers are not set";
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, null);
    }

    _brokers = props.getProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);

    // Assign mandatory arguments
    props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
    props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VAL_SERIALIZER);

    _producer = new KafkaProducer<>(props);

    // initialize metrics
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();
    _eventWriteRate = new Meter();
    _eventByteWriteRate = new Meter();
    _eventTransportErrorRate = new Meter();
  }

  private ProducerRecord<byte[], byte[]> convertToProducerRecord(KafkaDestination destination,
      DatastreamProducerRecord record, Object event) throws DatastreamException {

    Optional<Integer> partition = record.getPartition();

    byte[] keyValue = new byte[0];
    byte[] payloadValue = new byte[0];
    if (event instanceof BrooklinEnvelope) {
      BrooklinEnvelope envelope = (BrooklinEnvelope) event;
      if (envelope.getKey() instanceof byte[]) {
        keyValue = (byte[]) envelope.getKey();
      }

      if (envelope.getValue() instanceof byte[]) {
        payloadValue = (byte[]) envelope.getValue();
      }
    } else if (event instanceof byte[]) {
      payloadValue = (byte[]) event;
    }

    if (partition.isPresent() && partition.get() >= 0) {
      // If the partition is specified. We send the record to the specific partition
      return new ProducerRecord<>(destination.getTopicName(), partition.get(), keyValue, payloadValue);
    } else {
      // If the partition is not specified. We use the partitionKey as the key. Kafka will use the hash of that
      // to determine the partition.
      return new ProducerRecord<>(destination.getTopicName(), record.getPartitionKey().get().getBytes(), payloadValue);
    }
  }

  @Override
  public void send(String destinationUri, DatastreamProducerRecord record, SendCallback onSendComplete) {
    KafkaDestination destination = KafkaDestination.parse(destinationUri);
    try {
      Validate.notNull(record, "null event record.");
      Validate.notNull(record.getEvents(), "null datastream events.");
      Validate.isTrue(record.getPartition().isPresent() || record.getPartitionKey().isPresent(),
          "Either partition or partitionKey needs to be set");

      LOG.debug("Sending Datastream event record: %s", record);

      for (Object event : record.getEvents()) {
        ProducerRecord<byte[], byte[]> outgoing;
        try {
          outgoing = convertToProducerRecord(destination, record, event);
        } catch (Exception e) {
          String errorMessage = String.format("Failed to convert DatastreamEvent (%s) to ProducerRecord.", event);
          LOG.error(errorMessage, e);
          throw new DatastreamRuntimeException(errorMessage, e);
        }
        _eventWriteRate.mark();
        _eventByteWriteRate.mark(outgoing.key().length + outgoing.value().length);

        _producer.send(outgoing, (metadata, exception) -> onSendComplete.onCompletion(
            new DatastreamRecordMetadata(record.getCheckpoint(), metadata.topic(), metadata.partition()), exception));
        // Update topic-specific metrics and aggregate metrics
        int numBytes = outgoing.key().length + outgoing.value().length;
        _dynamicMetricsManager.createOrUpdateMeter(MODULE, destination.getTopicName(), EVENT_WRITE_RATE, 1);
        _dynamicMetricsManager.createOrUpdateMeter(MODULE, destination.getTopicName(), EVENT_BYTE_WRITE_RATE, numBytes);
        _dynamicMetricsManager.createOrUpdateMeter(MODULE, AGGREGATE, EVENT_WRITE_RATE, 1);
        _dynamicMetricsManager.createOrUpdateMeter(MODULE, AGGREGATE, EVENT_BYTE_WRITE_RATE, numBytes);
      }
    } catch (Exception e) {
      _eventTransportErrorRate.mark();
      _dynamicMetricsManager.createOrUpdateMeter(MODULE, destination.getTopicName(), EVENT_TRANSPORT_ERROR_RATE, 1);
      String errorMessage = String.format(
          "Sending event (%s) to topic %s and Kafka cluster (Metadata brokers) %s " + "failed with exception",
          record.getEvents(), destinationUri, _brokers);
      ErrorLogger.logAndThrowDatastreamRuntimeException(LOG, errorMessage, e);
    }

    LOG.debug("Done sending Datastream event record: %s", record);
  }

  @Override
  public void close() {
    // Close the kafka producer connection immediately (timeout of zero). If timeout > 0 producer connection won't
    // close immediately and will result in out of order events. More details in the below slide.
    // http://www.slideshare.net/JiangjieQin/no-data-loss-pipeline-with-apache-kafka-49753844/10
    if (_producer != null) {
      _producer.close(0, TimeUnit.MILLISECONDS);
    }
  }

  @Override
  public void flush() {
    _producer.flush();
  }
}
