package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import com.linkedin.kafka.clients.consumer.LiKafkaConsumerImpl;


/**
 * Factory that returns LiKafkaConsumer with no large message/auditing support. This is useful if a consumer doesn't
 * need to assemble large messages.
 */
public class LiKafkaConsumerNoLargeMessageFactory implements KafkaConsumerFactory<byte[], byte[]> {
  @Override
  public Consumer<byte[], byte[]> createConsumer(Properties properties) {
    properties.put("key.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    properties.put("value.deserializer", ByteArrayDeserializer.class.getCanonicalName());
    properties.put("segment.deserializer.class", NoOpSegmentDeserializer.class.getCanonicalName());
    properties.put("auditor.class", NoOpAuditor.class.getCanonicalName());
    return new LiKafkaConsumerImpl<>(properties);
  }
}



