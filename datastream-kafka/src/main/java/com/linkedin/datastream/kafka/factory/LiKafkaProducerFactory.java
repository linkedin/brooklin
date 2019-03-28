/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka.factory;

import com.linkedin.datastream.common.VerifiableProperties;
import com.linkedin.kafka.clients.producer.LiKafkaProducerConfig;
import com.linkedin.kafka.clients.producer.LiKafkaProducerImpl;
import java.util.Properties;
import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public class LiKafkaProducerFactory implements KafkaProducerFactory<byte[], byte[]> {
  // New producer configurations. Please look at http://kafka.apache.org/documentation.html#producerconfigs for
  // more details on what these configs mean.
  // The configs below should ensure that there is no data loss in the Kafka pipeline
  // http://www.slideshare.net/JiangjieQin/no-data-loss-pipeline-with-apache-kafka-49753844
  private static final String CFG_REQUEST_REQUIRED_ACKS = "acks";
  private static final String DEFAULT_REQUEST_REQUIRED_ACKS = "-1";

  private static final String CFG_REQUEST_TIMEOUT_MS = "request.timeout.ms";
  private static final String DEFAULT_REQUEST_TIMEOUT_MS = "120000"; // 120 seconds

  private static final String CFG_RETRY_BACKOFF_MS = "retry.backoff.ms";
  private static final String DEFAULT_RETRY_BACKOFF_MS = "2000"; // 2 seconds

  private static final String CFG_METADATA_EXPIRY_MS = "metadata.max.age.ms";
  private static final String DEFAULT_METADATA_EXPIRY_MS = "300000";  // 300 seconds

  // This is per partition batch size
  private static final String CFG_MAX_PARTITION_BYTES = "batch.size";
  private static final String DEFAULT_MAX_PARTITION_BYTES = "102400"; // 100 KB

  private static final String CFG_TOTAL_MEMORY_BYTES = "buffer.memory";
  private static final String DEFAULT_TOTAL_MEMORY_BYTES = "524288000"; // 512 MB

  // Time to wait for batching the sends. Since we have max.in.flight.requests.per.connection to 1. We get batching
  // through that ala Nagle algorithm, Hence turning this off by setting it to 0.
  private static final String CFG_LINGER_MS = "linger.ms";
  private static final String DEFAULT_LINGER_MS = "0";

  // Size of the socket buffer used while sending data to the broker.
  private static final String CFG_SEND_BUFFER_BYTES = "send.buffer.bytes";
  private static final String DEFAULT_SEND_BUFFER_BYTES = "131072"; // 128 KB

  // Size of the socket buffer used while receiving data from the broker.
  private static final String CFG_RECEIVE_BUFFER_BYTES = "receive.buffer.bytes";
  private static final String DEFAULT_RECEIVE_BUFFER_BYTES = "131072"; // 128 KB

  private static final String CFG_MAX_REQUEST_SIZE = "max.request.size";
  private static final String DEFAULT_MAX_REQUEST_SIZE = "104857600"; // 100 MB

  private static final String CFG_RECONNECT_BACKOFF_MS = "reconnect.backoff.ms";
  private static final String DEFAULT_RECONNECT_BACKOFF_MS = "500";

  private static final String CFG_MAX_BLOCK_MS = "max.block.ms";
  private static final String DEFAULT_MAX_BLOCK_MS = String.valueOf(Integer.MAX_VALUE);

  private static final String CFG_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "max.in.flight.requests.per.connection";
  private static final String DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION = "1";

  private static final String CFG_REQUEST_RETRIES = "retries";
  private static final String DEFAULT_REQUEST_RETRIES = String.valueOf(Integer.MAX_VALUE);

  private static final String CFG_COMPRESSION_TYPE = "compression.type";
  private static final String DEFAULT_COMPRESSION_TYPE = "gzip";

  private static final String DEFAULT_ENABLE_LARGE_MESSAGE = "false";
  /* Package Visible */
  static Properties buildProducerProperties(Properties prop, String clientId, String brokers, String enableLargeMessage) {
    prop.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    prop.put(LiKafkaProducerConfig.LARGE_MESSAGE_ENABLED_CONFIG, enableLargeMessage);
    prop.putIfAbsent(CFG_RETRY_BACKOFF_MS, DEFAULT_RETRY_BACKOFF_MS);
    prop.putIfAbsent(CFG_REQUEST_TIMEOUT_MS, DEFAULT_REQUEST_TIMEOUT_MS);
    prop.putIfAbsent(CFG_METADATA_EXPIRY_MS, DEFAULT_METADATA_EXPIRY_MS);
    prop.putIfAbsent(CFG_MAX_PARTITION_BYTES, DEFAULT_MAX_PARTITION_BYTES);
    prop.putIfAbsent(CFG_TOTAL_MEMORY_BYTES, DEFAULT_TOTAL_MEMORY_BYTES);
    prop.putIfAbsent(CFG_REQUEST_REQUIRED_ACKS, DEFAULT_REQUEST_REQUIRED_ACKS);
    prop.putIfAbsent(CFG_LINGER_MS, DEFAULT_LINGER_MS);
    prop.putIfAbsent(CFG_SEND_BUFFER_BYTES, DEFAULT_SEND_BUFFER_BYTES);
    prop.putIfAbsent(CFG_RECEIVE_BUFFER_BYTES, DEFAULT_RECEIVE_BUFFER_BYTES);
    prop.putIfAbsent(CFG_MAX_REQUEST_SIZE, DEFAULT_MAX_REQUEST_SIZE);
    prop.putIfAbsent(CFG_RECONNECT_BACKOFF_MS, DEFAULT_RECONNECT_BACKOFF_MS);
    prop.putIfAbsent(CFG_MAX_BLOCK_MS, DEFAULT_MAX_BLOCK_MS);
    prop.putIfAbsent(CFG_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, DEFAULT_MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
    prop.putIfAbsent(CFG_REQUEST_RETRIES, DEFAULT_REQUEST_RETRIES);
    prop.putIfAbsent(CFG_COMPRESSION_TYPE, DEFAULT_COMPRESSION_TYPE);
    return prop;
  }

  @Override
  public Producer<byte[], byte[]> createProducer(Properties transportProps) {
    VerifiableProperties transportProviderProperties = new VerifiableProperties(transportProps);
    String clientId = transportProviderProperties.getString(ProducerConfig.CLIENT_ID_CONFIG);
    String bootstrapServers = transportProviderProperties.getString(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
    Properties producerConfig = transportProviderProperties.getDomainProperties(DOMAIN_PRODUCER);

    Validate.notEmpty(clientId, "clientId cannot be empty.");
    Validate.notEmpty(bootstrapServers, "bootstrapServers cannot be empty.");

    producerConfig = buildProducerProperties(producerConfig, clientId, bootstrapServers, DEFAULT_ENABLE_LARGE_MESSAGE);

    // Default DeSerializer for Key and Payload
    producerConfig.putIfAbsent(LiKafkaProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getCanonicalName());
    producerConfig.putIfAbsent(LiKafkaProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
        ByteArraySerializer.class.getCanonicalName());

    return new LiKafkaProducerImpl<byte[], byte[]>(producerConfig);
  }
}
