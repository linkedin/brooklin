package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.StringJoiner;

import com.google.common.annotations.VisibleForTesting;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.BrooklinEnvelope;
import com.linkedin.datastream.common.BrooklinEnvelopeMetadataConstants;
import com.linkedin.datastream.connectors.CommonConnectorMetrics;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamProducerRecord;
import com.linkedin.datastream.server.DatastreamProducerRecordBuilder;
import com.linkedin.datastream.server.DatastreamTask;


public class KafkaConnectorTask extends AbstractKafkaBasedConnectorTask {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConnectorTask.class);
  private static final String CLASS_NAME = KafkaConnectorTask.class.getSimpleName();
  // Regular expression to capture all metrics by this kafka connector.
  private static final String METRICS_PREFIX_REGEX = CLASS_NAME + MetricsAware.KEY_REGEX;

  private KafkaConnectionString _srcConnString =
      KafkaConnectionString.valueOf(_task.getDatastreamSource().getConnectionString());

  public KafkaConnectorTask(KafkaConsumerFactory<?, ?> factory, Properties consumerProps, DatastreamTask task,
      long commitIntervalMillis, Duration retrySleepDuration, int retryCount) {
    super(factory, consumerProps, task, commitIntervalMillis, retrySleepDuration, retryCount, LOG);
  }

  @VisibleForTesting
  static Properties getKafkaConsumerProperties(Properties consumerProps, String groupId,
      KafkaConnectionString connectionString) {
    StringJoiner csv = new StringJoiner(",");
    connectionString.getBrokers().forEach(broker -> csv.add(broker.toString()));
    String bootstrapValue = csv.toString();

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapValue);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
    props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); //auto-commits are unsafe
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none");
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, connectionString.isSecure() ? "SSL" : "PLAINTEXT");
    props.putAll(consumerProps);
    return props;
  }

  @Override
  void consumerSubscribe() {
    KafkaConnectionString srcConnString =
        KafkaConnectionString.valueOf(_task.getDatastreamSource().getConnectionString());
    _consumer.subscribe(Collections.singletonList(srcConnString.getTopicName()), this);
  }

  @Override
  void translateAndSendBatch(ConsumerRecords<?, ?> records, Instant readTime) throws InterruptedException {
    for (ConsumerRecord<?, ?> record : records) {
      try {
        sendMessage(record, readTime.toEpochMilli(), String.valueOf(readTime.toEpochMilli()));
      } catch (RuntimeException e) {
        _consumerMetrics.updateErrorRate(1);
        // Throw the exception and let the connector rewind and retry.
        throw e;
      }
    }
  }

  public static Consumer<?, ?> createConsumer(KafkaConsumerFactory<?, ?> consumerFactory, Properties consumerProps,
      String groupId, KafkaConnectionString connectionString) {

    Properties props = getKafkaConsumerProperties(consumerProps, groupId, connectionString);
    return consumerFactory.createConsumer(props);
  }

  @Override
  Consumer<?, ?> createKafkaConsumer(KafkaConsumerFactory<?, ?> consumerFactory, Properties consumerProps) {
    return createConsumer(consumerFactory, consumerProps, getKafkaGroupId(_task), _srcConnString);
  }

  @VisibleForTesting
  static String getKafkaGroupId(DatastreamTask task) {
    KafkaConnectionString srcConnString =
        KafkaConnectionString.valueOf(task.getDatastreamSource().getConnectionString());
    String dstConnString = task.getDatastreamDestination().getConnectionString();

    return task.getDatastreams()
        .stream()
        .map(ds -> ds.getMetadata().get(ConsumerConfig.GROUP_ID_CONFIG))
        .filter(Objects::nonNull)
        .findFirst()
        .orElse(srcConnString + "-to-" + dstConnString);
  }

  public static List<BrooklinMetricInfo> getMetricInfos() {
    List<BrooklinMetricInfo> metrics = new ArrayList<>();
    metrics.addAll(CommonConnectorMetrics.getEventProcessingMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getEventPollMetrics(METRICS_PREFIX_REGEX));
    metrics.addAll(CommonConnectorMetrics.getPartitionSpecificMetrics(METRICS_PREFIX_REGEX));
    return metrics;
  }

  private void sendMessage(ConsumerRecord<?, ?> record, long readTime, String strReadTime) throws InterruptedException {
    int count = 0;
    while (true) {
      count++;
      try {
        int numBytes = record.serializedKeySize() + record.serializedValueSize();
        _consumerMetrics.updateBytesProcessedRate(numBytes);
        _producer.send(translate(record, readTime, strReadTime), null);
        return; // Break the retry loop and exit.
      } catch (RuntimeException e) {
        LOG.error("Error sending Message. task: {} ; error: {};", _taskName, e.toString());
        LOG.error("Stack Trace: {}", Arrays.toString(e.getStackTrace()));
        if (_shouldDie || count >= _retryCount) {
          LOG.info("Send messages failed with exception: {}", e);
          throw e;
        }
        LOG.warn("Sleeping for {} seconds before retrying. Retry {} of {}",
            _retrySleepDuration.getSeconds(), count, _retryCount);
        Thread.sleep(_retrySleepDuration.toMillis());
      }
    }
  }

  private DatastreamProducerRecord translate(ConsumerRecord<?, ?> fromKafka, long readTime, String strReadTime) {
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put("kafka-origin", _srcConnString.toString());
    int partition = fromKafka.partition();
    String partitionStr = String.valueOf(partition);
    metadata.put("kafka-origin-partition", partitionStr);
    String offsetStr = String.valueOf(fromKafka.offset());
    metadata.put("kafka-origin-offset", offsetStr);
    metadata.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, strReadTime);
    BrooklinEnvelope envelope = new BrooklinEnvelope(fromKafka.key(), fromKafka.value(), null, metadata);
    //TODO - copy over headers if/when they are ever supported
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(envelope);
    builder.setEventsSourceTimestamp(readTime);
    builder.setPartition(partition); //assume source partition count is same as dest
    builder.setSourceCheckpoint(partitionStr + "-" + offsetStr);

    return builder.build();
  }
}
