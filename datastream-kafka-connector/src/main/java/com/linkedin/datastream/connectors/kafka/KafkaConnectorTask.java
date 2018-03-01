package com.linkedin.datastream.connectors.kafka;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
      KafkaConnectionString.valueOf(_datastreamTask.getDatastreamSource().getConnectionString());
  private final KafkaConsumerFactory<?, ?> _consumerFactory;

  public KafkaConnectorTask(KafkaConsumerFactory<?, ?> factory, Properties consumerProps, DatastreamTask task,
      long commitIntervalMillis, Duration retrySleepDuration, int retryCount, boolean pausePartitionOnRetryExhaustion) {
    super(consumerProps, task, commitIntervalMillis, retrySleepDuration, retryCount, pausePartitionOnRetryExhaustion,
        LOG);
    _consumerFactory = factory;
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
  protected void consumerSubscribe() {
    KafkaConnectionString srcConnString =
        KafkaConnectionString.valueOf(_datastreamTask.getDatastreamSource().getConnectionString());
    _consumer.subscribe(Collections.singletonList(srcConnString.getTopicName()), this);
  }

  public static Consumer<?, ?> createConsumer(KafkaConsumerFactory<?, ?> consumerFactory, Properties consumerProps,
      String groupId, KafkaConnectionString connectionString) {

    Properties props = getKafkaConsumerProperties(consumerProps, groupId, connectionString);
    return consumerFactory.createConsumer(props);
  }

  @Override
  protected Consumer<?, ?> createKafkaConsumer(Properties consumerProps) {
    return createConsumer(_consumerFactory, consumerProps, getKafkaGroupId(_datastreamTask), _srcConnString);
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

  @Override
  protected DatastreamProducerRecord translate(ConsumerRecord<?, ?> fromKafka, Instant readTime) {
    HashMap<String, String> metadata = new HashMap<>();
    metadata.put("kafka-origin", _srcConnString.toString());
    int partition = fromKafka.partition();
    String partitionStr = String.valueOf(partition);
    metadata.put("kafka-origin-partition", partitionStr);
    String offsetStr = String.valueOf(fromKafka.offset());
    metadata.put("kafka-origin-offset", offsetStr);
    metadata.put(BrooklinEnvelopeMetadataConstants.EVENT_TIMESTAMP, String.valueOf(readTime.toEpochMilli()));
    BrooklinEnvelope envelope = new BrooklinEnvelope(fromKafka.key(), fromKafka.value(), null, metadata);
    //TODO - copy over headers if/when they are ever supported
    DatastreamProducerRecordBuilder builder = new DatastreamProducerRecordBuilder();
    builder.addEvent(envelope);
    builder.setEventsSourceTimestamp(readTime.toEpochMilli());
    builder.setPartition(partition); //assume source partition count is same as dest
    builder.setSourceCheckpoint(partitionStr + "-" + offsetStr);

    return builder.build();
  }
}