package com.linkedin.datastream;

/*
 *
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
 *
 */

import java.io.IOException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import com.linkedin.datastream.avro.AvroUtils;
import com.linkedin.datastream.avro.DatastreamEvent;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;

public class DatastreamRecords {

  private final ConsumerRecords<String, byte[]> _records;

  DatastreamRecords(ConsumerRecords<String, byte[]> records) {
    _records = records;
  }

  public int count() {
    return _records.count();
  }

  public Iterable<DatastreamEvent> records(DatabaseTablePartition dtp) {
    TopicPartition topicPartition = DatastreamConsumerImpl.getTopicPartition(dtp);
    Iterable<ConsumerRecord<String, byte[]>> recordsInPartition = _records.records(topicPartition);
    return Iterables.transform(recordsInPartition, new Function<ConsumerRecord<String,byte[]>, DatastreamEvent>() {
      @Override
      public DatastreamEvent apply(ConsumerRecord<String, byte[]> input) throws RuntimeException {
        try {
          return AvroUtils.decodeAvroSpecificRecord(DatastreamEvent.class, input.value());
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    });
  }
}
