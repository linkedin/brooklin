/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka.factory;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;


public interface KafkaProducerFactory<K, V> {
  String DOMAIN_PRODUCER = "producer";

  /**
   * Create a Kafka Producer using the Transport Provider Properties.
   * The "Standard" Kafka Producer Properties will have the {@link DOMAIN_PRODUCER} prefix.
   */
  Producer<K, V> createProducer(Properties transportProps);
}
