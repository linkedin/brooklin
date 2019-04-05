/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


/**
 * Helper class for writing unit tests with EmbeddedKafka.
 */
public final class KafkaTestUtils {
  private static final int DEFAULT_TIMEOUT_MS = 60000;
  private static AtomicInteger _groupCounter = new AtomicInteger();


  /**
   * Interface for the callback used as reader ( ReaderCallback)
   * used to make sure the callback implements onMessage API
   */
  public interface ReaderCallback {

    /**
     * API for the reader callback to consume an incoming message
     */
    boolean onMessage(byte[] key, byte[] value) throws IOException;
  }

  private KafkaTestUtils() {
  }

  /**
   * Get a List of PartitionInfo for partitions of a given topic
   */
  public static List<PartitionInfo> getPartitionInfo(String topic, String brokerList) {
    KafkaConsumer<byte[], byte[]> consumer = createConsumer(brokerList);
    return consumer.partitionsFor(topic);
  }

  /**
   * consume events from a given partition of a kafka topic, using given ReaderCallback
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
    String groupId = "test_" + _groupCounter.incrementAndGet();
    props.put("bootstrap.servers", brokerList);
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("group.id", groupId);
    props.put("auto.offset.reset", "earliest");


    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    return consumer;
  }
}
