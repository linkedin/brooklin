package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;


public interface KafkaConsumerFactory<K, V> {
  Consumer<K, V> createConsumer(Properties properties);
}
