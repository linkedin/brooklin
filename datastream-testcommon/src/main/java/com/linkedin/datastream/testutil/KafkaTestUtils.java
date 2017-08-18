package com.linkedin.datastream.testutil;

import java.util.Collections;

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeoutException;

import com.linkedin.datastream.common.PollUtils;


/**
 * Helper class for writing unit tests with EmbeddedKafka.
 */
public final class KafkaTestUtils {
  private static final int DEFAULT_TIMEOUT_MS = 60000;

  public interface ReaderCallback {
    boolean onMessage(byte[] key, byte[] value) throws IOException;
  }

  private KafkaTestUtils() {
  }

  public static List<PartitionInfo> getPartitionInfo(String topic, String brokerList) {
    KafkaConsumer<byte[], byte[]> consumer = createConsumer(brokerList);
    return consumer.partitionsFor(topic);
  }

  public static void readTopic(String topic, Integer partition, String brokerList, ReaderCallback callback)
      throws Exception {
    Validate.notNull(topic);
    Validate.notNull(partition);
    Validate.notNull(brokerList);
    Validate.notNull(callback);

    TopicPartition subscription = new TopicPartition(topic, partition);
    List<TopicPartition> topicPartitions = Collections.singletonList(subscription);
    KafkaConsumer<byte[], byte[]> consumer = createConsumer(brokerList);
    consumer.assign(topicPartitions);
    consumer.seekToBeginning(Collections.singletonList(subscription));

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
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", "test");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    return consumer;
  }

  /**
   * Ensure a topic is ready, by listing the topic from this cluster, and making sure it is present
   * with the right number of partitions.
   * @param topicName
   * @param partitions
   * @throws AssertionError if the topic is not ready after {@code DEFAULT_TIMEOUT_MS}
   */
  public static void ensureTopicIsReady(String brokers, String topicName, int partitions) {
    final KafkaConsumer<?, ?> kafkaConsumer = createConsumer(brokers);
    if (!PollUtils.poll(() -> isTopicReady(topicName, partitions, kafkaConsumer), 1000, DEFAULT_TIMEOUT_MS)) {
      throw new RuntimeException(String.format(
          "Topic %s is not ready after waiting for %d milliseconds. existingTopics=%s",
          topicName, DEFAULT_TIMEOUT_MS, kafkaConsumer.listTopics()));
    }
  }

  private static boolean isTopicReady(String topicName, int partitions, KafkaConsumer<?, ?> consumer) {
    Map<String, List<PartitionInfo>> list = consumer.listTopics();
    return list.containsKey(topicName) && list.get(topicName).size() == partitions;
  }
}
