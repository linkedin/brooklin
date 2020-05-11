/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka.mirrormaker;

import java.time.Duration;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.Charsets;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.Assert;

import com.linkedin.data.template.StringMap;
import com.linkedin.datastream.common.Datastream;
import com.linkedin.datastream.common.DatastreamDestination;
import com.linkedin.datastream.common.DatastreamMetadataConstants;
import com.linkedin.datastream.common.DatastreamSource;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfig;
import com.linkedin.datastream.connectors.kafka.KafkaBasedConnectorConfigBuilder;
import com.linkedin.datastream.connectors.kafka.LiKafkaConsumerFactory;
import com.linkedin.datastream.connectors.kafka.NoOpAuditor;
import com.linkedin.datastream.connectors.kafka.NoOpSegmentDeserializer;
import com.linkedin.datastream.server.DatastreamTaskImpl;
import com.linkedin.datastream.testutil.DatastreamEmbeddedZookeeperKafkaCluster;


final class KafkaMirrorMakerConnectorTestUtils {

  static final long POLL_PERIOD_MS = Duration.ofMillis(100).toMillis();
  static final long POLL_TIMEOUT_MS = Duration.ofSeconds(30).toMillis();

  static Properties getKafkaProducerProperties(DatastreamEmbeddedZookeeperKafkaCluster kafkaCluster) {
    Properties props = new Properties();
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaCluster.getBrokers());
    props.put(ProducerConfig.ACKS_CONFIG, "all");
    props.put(ProducerConfig.RETRIES_CONFIG, 100);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
    props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
    props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());

    return props;
  }

  static void produceEvents(String topic, int destinationPartition, int numEvents,
      DatastreamEmbeddedZookeeperKafkaCluster kafkaCluster) {
    produceEventsToPartition(topic, destinationPartition, numEvents, kafkaCluster);
  }

  static void produceEvents(String topic, int numEvents, DatastreamEmbeddedZookeeperKafkaCluster kafkaCluster) {
    produceEventsToPartition(topic, null, numEvents, kafkaCluster);
  }

  static void produceEventsToPartition(String topic, Integer destinationPartition, int numEvents,
      DatastreamEmbeddedZookeeperKafkaCluster kafkaCluster) {
    try (Producer<byte[], byte[]> producer = new KafkaProducer<>(getKafkaProducerProperties(kafkaCluster))) {
      for (int i = 0; i < numEvents; i++) {
        producer.send(new ProducerRecord<>(topic, destinationPartition, ("key-" + i).getBytes(Charsets.UTF_8),
            ("value-" + i).getBytes(Charsets.UTF_8)), (metadata, exception) -> {
          if (exception != null) {
            throw new RuntimeException("Failed to send message.", exception);
          }
        });
      }
      producer.flush();
    }
  }

  static Datastream createDatastream(String name, String broker, String sourceRegex, StringMap metadata) {
    DatastreamSource source = new DatastreamSource();
    source.setConnectionString("kafka://" + broker + "/" + sourceRegex);
    Datastream datastream = new Datastream();
    datastream.setName(name);
    datastream.setConnectorName("KafkaMirrorMaker");
    datastream.setSource(source);
    datastream.setMetadata(metadata);
    datastream.setTransportProviderName("transportProvider");
    return datastream;
  }

  static Datastream createDatastream(String name, String broker, String sourceRegex) {
    StringMap metadata = new StringMap();
    metadata.put(DatastreamMetadataConstants.REUSE_EXISTING_DESTINATION_KEY, Boolean.FALSE.toString());
    metadata.put(DatastreamMetadataConstants.TASK_PREFIX, UUID.randomUUID().toString());
    Datastream datastream = createDatastream(name, broker, sourceRegex, metadata);
    DatastreamDestination destination = new DatastreamDestination();
    destination.setConnectionString(KafkaMirrorMakerConnector.MM_TOPIC_PLACEHOLDER);
    datastream.setDestination(destination);
    return datastream;
  }

  static KafkaMirrorMakerConnectorTask createKafkaMirrorMakerConnectorTask(DatastreamTaskImpl task) {
    return createKafkaMirrorMakerConnectorTask(task, getKafkaBasedConnectorConfigBuilder().build());
  }

  static KafkaMirrorMakerConnectorTask createKafkaMirrorMakerConnectorTask(DatastreamTaskImpl task,
      KafkaBasedConnectorConfig connectorConfig) {
    return new KafkaMirrorMakerConnectorTask(connectorConfig, task, "", false,
        new KafkaMirrorMakerGroupIdConstructor(false, "testCluster"));
  }

  static KafkaMirrorMakerConnectorTask createFlushlessKafkaMirrorMakerConnectorTask(DatastreamTaskImpl task,
      boolean flowControlEnabled, long autoResumeThreshold, long autoPauseThreshold,
      Duration pauseErrorPartitionDuration) {
    Properties connectorProps = new Properties();
    connectorProps.put(KafkaMirrorMakerConnectorTask.CONFIG_FLOW_CONTROL_ENABLED, String.valueOf(flowControlEnabled));
    connectorProps.put(KafkaMirrorMakerConnectorTask.CONFIG_MIN_IN_FLIGHT_MSGS_THRESHOLD,
        String.valueOf(autoResumeThreshold));
    connectorProps.put(KafkaMirrorMakerConnectorTask.CONFIG_MAX_IN_FLIGHT_MSGS_THRESHOLD,
        String.valueOf(autoPauseThreshold));

    KafkaBasedConnectorConfig connectorConfig = getKafkaBasedConnectorConfigBuilder()
        .setConsumerFactory(new LiKafkaConsumerFactory())
        .setConnectorProps(connectorProps)
        .setRetryCount(0)
        .setPauseErrorPartitionDuration(pauseErrorPartitionDuration)
        .build();

    return new KafkaMirrorMakerConnectorTask(connectorConfig, task, "", true,
        new KafkaMirrorMakerGroupIdConstructor(false, "testCluster"));
  }

  static Thread runKafkaMirrorMakerConnectorTask(KafkaMirrorMakerConnectorTask connectorTask)
      throws InterruptedException {
    Thread t = new Thread(connectorTask, "connector thread");
    t.setDaemon(true);
    t.setUncaughtExceptionHandler((t1, e) -> Assert.fail("connector thread died", e));
    t.start();
    if (!connectorTask.awaitStart(60, TimeUnit.SECONDS)) {
      Assert.fail("connector did not start within timeout");
    }
    return t;
  }

  static KafkaBasedConnectorConfigBuilder getKafkaBasedConnectorConfigBuilder() {
    return new KafkaBasedConnectorConfigBuilder()
        .setConsumerProps(getKafkaConsumerProperties())
        .setPausePartitionOnError(true)
        .setPauseErrorPartitionDuration(Duration.ofSeconds(5));
  }

  /**
   * Returns properties that will be used to configure Kafka consumer in BMM.
   * Right now it returns No Op Segment Deserializer and No Op Auditor, as BMM doesn't need to assemble/disassemble
   * any message, it just needs to do byte-byte copying.
   * @return Properties to be used by Kafka consumer in BMM.
   */
  static Properties getKafkaConsumerProperties() {
    Properties props = new Properties();
    props.put("segment.deserializer.class", NoOpSegmentDeserializer.class.getCanonicalName());
    props.put("auditor.class", NoOpAuditor.class.getCanonicalName());
    return props;
  }

  /**
   * Get the default config properties of a Kafka-based connector
   * @param override Configuration properties to override default config properties
   */
  public static Properties getDefaultConfig(Optional<Properties> override) {
    Properties config = new Properties();
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_KEY_SERDE, "keySerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_DEFAULT_VALUE_SERDE, "valueSerde");
    config.put(KafkaBasedConnectorConfig.CONFIG_COMMIT_INTERVAL_MILLIS, "10000");
    config.put(KafkaBasedConnectorConfig.CONFIG_COMMIT_TIMEOUT_MILLIS, "1000");
    config.put(KafkaBasedConnectorConfig.CONFIG_POLL_TIMEOUT_MILLIS, "5000");
    config.put(KafkaBasedConnectorConfig.CONFIG_CONSUMER_FACTORY_CLASS, LiKafkaConsumerFactory.class.getName());
    config.put(KafkaBasedConnectorConfig.CONFIG_PAUSE_PARTITION_ON_ERROR, Boolean.TRUE.toString());
    config.put(KafkaBasedConnectorConfig.CONFIG_RETRY_SLEEP_DURATION_MILLIS, "1000");
    config.put(KafkaBasedConnectorConfig.CONFIG_PAUSE_ERROR_PARTITION_DURATION_MILLIS,
        String.valueOf(Duration.ofSeconds(5).toMillis()));
    override.ifPresent(config::putAll);
    return config;
  }
}
