/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import kafka.admin.AdminUtils;
import kafka.utils.ZkUtils;


/**
 * Helper class for writing unit tests with EmbeddedKafka.
 */
public final class KafkaTestUtils {
  private static final int DEFAULT_TIMEOUT_MS = 60000;
  private static final AtomicInteger GROUP_COUNTER = new AtomicInteger();


  /**
   * Interface for the callback invoked whenever messages are read
   */
  public interface ReaderCallback {

    /**
     * Callback invoked whenever a message is read to so it can be consumed
     */
    boolean onMessage(byte[] key, byte[] value) throws IOException;
  }

  private KafkaTestUtils() {
  }

  /**
   * Get all topic-partition info of a given Kafka topic on a given set of Kafka brokers
   */
  public static List<PartitionInfo> getPartitionInfo(String topic, String brokerList) {
    KafkaConsumer<byte[], byte[]> consumer = createConsumer(brokerList);
    return consumer.partitionsFor(topic);
  }

  /**
   * Waits for a topic to be created and ready for production by waiting for the topic to be created and then attempting
   * to consume from it once it is created.
   *
   * Note: The topic creation must be issued before this method is called.
   *
   * @param topic the topic to wait for broker assignment
   * @param brokerList the brokers in the Kafka cluster
   * @throws IllegalStateException if the topic is not ready before the timeout ({@value #DEFAULT_TIMEOUT_MS} ms)
   */
  public static void waitForTopicCreation(ZkUtils zkUtils, String topic, String brokerList) throws IllegalStateException {
    Validate.notNull(zkUtils);
    Validate.notEmpty(topic);
    Validate.notEmpty(brokerList);

    Duration attemptDuration = Duration.ofMillis(DEFAULT_TIMEOUT_MS);
    Instant expiration = Instant.now().plus(attemptDuration);

    // Wait for topic creation
    while (Instant.now().isBefore(expiration) && !AdminUtils.topicExists(zkUtils, topic)) {
      try {
        Thread.sleep(Duration.ofSeconds(1).toMillis());
      } catch (InterruptedException e) {
        throw new IllegalStateException("Interrupted before topic creation could be verified", e);
      }
    }

    if (Instant.now().isAfter(expiration)) {
      throw new IllegalStateException("Topic was not created within the timeout");
    }

    // Ensure topic is consumable (ready)
    try (KafkaConsumer<byte[], byte[]> consumer = createConsumer(brokerList)) {
      consumer.subscribe(Collections.singleton(topic));
      while (Instant.now().isBefore(expiration)) {
        try {
          consumer.poll(Duration.ofSeconds(1).toMillis());
          return;
        } catch (Exception ignored) {
          // Exception should occur when we are waiting for the broker to be assigned this topic
        }
      }
    }

    throw new IllegalStateException("Topic was not ready within the timeout");
  }

  /**
   * Consume messages from a given partition of a Kafka topic, using given ReaderCallback
   */
  public static void readTopic(String topic, Integer partition, String brokerList, ReaderCallback callback)
      throws Exception {
    Validate.notNull(topic);
    Validate.notNull(partition);
    Validate.notNull(brokerList);
    Validate.notNull(callback);

    KafkaConsumer<byte[], byte[]> consumer = createConsumer(brokerList);
    if (partition >= 0) {
      List<TopicPartition> topicPartitions = Collections.singletonList(new TopicPartition(topic, partition));
      consumer.assign(topicPartitions);
      consumer.seekToBeginning(topicPartitions);
    } else {
      consumer.subscribe(Collections.singletonList(topic));
    }

    boolean keepGoing = true;
    long now = System.currentTimeMillis();
    do {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
      for (ConsumerRecord<byte[], byte[]> record : records.records(topic)) {
        if (!callback.onMessage(record.key(), record.value())) {
          keepGoing = false;
          break;
        }
      }

      // Guard against buggy test which can hang forever
      if (System.currentTimeMillis() - now >= DEFAULT_TIMEOUT_MS) {
        throw new TimeoutException("Timed out before reading all messages");
      }
    } while (keepGoing);
  }

  private static KafkaConsumer<byte[], byte[]> createConsumer(String brokerList) {

    Properties props = new Properties();
    String groupId = "test_" + GROUP_COUNTER.incrementAndGet();
    props.put("bootstrap.servers", brokerList);
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("group.id", groupId);
    props.put("auto.offset.reset", "earliest");

    return new KafkaConsumer<>(props);
  }
}
