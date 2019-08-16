/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;


/**
 * A factory for creating Kafka {@link Consumer} instances
 */
public class KafkaConsumerFactoryImpl implements KafkaConsumerFactory<byte[], byte[]> {
  private static final String KEY_DESERIALIZER = ByteArrayDeserializer.class.getCanonicalName();
  private static final String VAL_DESERIALIZER = ByteArrayDeserializer.class.getCanonicalName();

  /* Package Visible */
  static Properties addConsumerDefaultProperties(Properties properties) {
    properties.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            KEY_DESERIALIZER);
    properties.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            VAL_DESERIALIZER);
    return properties;
  }

  @Override
  public Consumer<byte[], byte[]> createConsumer(Properties properties) {
    addConsumerDefaultProperties(properties);
    return new KafkaConsumer<>(properties);
  }
}
