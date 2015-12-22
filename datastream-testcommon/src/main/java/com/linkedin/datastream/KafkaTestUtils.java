package com.linkedin.datastream;

/*
 * Copyright 2015 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.commons.lang.Validate;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;


/**
 * Helper class for writing unit tests with EmbeddedKafka.
 */
public final class KafkaTestUtils {
  public interface ReaderCallback {
    boolean onMessage(byte[] key, byte[] value)
        throws IOException;
  }

  private KafkaTestUtils() {
  }

  public static void readTopic(String topic, Integer partition, String brokerList, ReaderCallback callback)
      throws Exception {
    Validate.notNull(topic);
    Validate.notNull(partition);
    Validate.notNull(brokerList);
    Validate.notNull(callback);

    Properties props = new Properties();
    props.put("bootstrap.servers", brokerList);
    props.put("group.id", "test");
    props.put("auto.commit.interval.ms", "1000");
    props.put("session.timeout.ms", "30000");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

    KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props);
    TopicPartition subscription = new TopicPartition(topic, partition);
    List<TopicPartition> topicPartitions = new ArrayList<TopicPartition>() { {
      add(subscription);
    } };
    consumer.assign(topicPartitions);
    consumer.seekToBeginning(subscription);

    boolean keepGoing = true;
    do {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(1000);
      for (ConsumerRecord<byte[], byte[]> record : records.records(topic)) {
        if (!callback.onMessage(record.key(), record.value())) {
          keepGoing = false;
          break;
        }
      }
    } while (keepGoing);
  }
}
