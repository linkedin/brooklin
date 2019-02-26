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


/**
 * Factory class for SimpleKafkaProducerFactory
 */
public class SimpleKafkaProducerFactory implements KafkaProducerFactory<byte[], byte[]> {
  private static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";
  private static final String VAL_SERIALIZER = "org.apache.kafka.common.serialization.ByteArraySerializer";

  @Override
  public Producer<byte[], byte[]> createProducer(Properties transportProps) {
    transportProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
    transportProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VAL_SERIALIZER);

    return new KafkaProducer<>(transportProps);
  }
}
