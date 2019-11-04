/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka.factory;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;

/**
 * Interface for Kafka consumer factories
 * @param <K> The type that the key is deserialized into
 * @param <V> The type that the value is deserialized into
 */
public interface KafkaConsumerFactory<K, V> {
  /**
  * Create a consumer instance using given {@link Properties}
  */
  Consumer<K, V> createConsumer(Properties properties);
}
