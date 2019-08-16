/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka.factory;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;


/**
 * Factory class for SimpleKafkaProducerFactory
 */
public class SimpleKafkaProducerFactory implements KafkaProducerFactory<byte[], byte[]> {
  private static final String KEY_SERIALIZER = ByteArraySerializer.class.getCanonicalName();
  private static final String VAL_SERIALIZER = ByteArraySerializer.class.getCanonicalName();

  /* Package Visible */
  static Properties addProducerDefaultProperties(Properties properties) {
    properties.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
    properties.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VAL_SERIALIZER);
    return properties;
  }

  @Override
  public Producer<byte[], byte[]> createProducer(Properties transportProps) {
    addProducerDefaultProperties(transportProps);
    return new KafkaProducer<>(transportProps);
  }
}
