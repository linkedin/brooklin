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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;


import sun.reflect.generics.reflectiveObjects.NotImplementedException;


public class DatastreamConsumerImpl implements DatastreamConsumer{

  private static final String DatabaseTopicNameFmt = "Datastream-%s-%s";
  private static final String TableTopicNameFmt = "Datastream-%s-%s-%s";

  private final KafkaConsumer<String, byte[]> _kafkaConsumer;

  public DatastreamConsumerImpl(KafkaConsumer<String, byte[]> consumer) {
    _kafkaConsumer = consumer;
  }

  @Override
  public void commit(Collection<Checkpoint> checkpoints, CommitType commitType) {

    HashMap<TopicPartition, Long> offsets = new HashMap<>();
    for(Checkpoint c : checkpoints) {
      offsets.put(getTopicPartition(c.getDatabaseTablePartition()), c.getOffset());
    }

    _kafkaConsumer.commit(offsets, commitType);
  }

  @Override
  public void commit(CommitType commitType) {
    _kafkaConsumer.commit(commitType);
  }

  @Override
  public void subscribe(DatabaseTablePartition... databaseTablePartitions) {
    _kafkaConsumer.subscribe(getTopicPartitions(databaseTablePartitions));
  }

  @Override
  public void subscribeToTables(String... tables) {
    String[] tableTopicNames = new String[tables.length];
    int index = 0;
    for(String tableName : tables) {
      String[] tableParts = tableName.split(".", 3);
      tableTopicNames[index] = getTableTopicName(tableParts[0], tableParts[1], tableParts[2]);
      index++;
    }

    _kafkaConsumer.subscribe(tableTopicNames);
  }

  @Override
  public void subscribeToDatabases(String... databases) {
    String[] databaseTopicNames = new String[databases.length];
    int index = 0;
    for(String databaseName : databases) {
      String[] databaseNameParts = databaseName.split(".", 2);
      databaseTopicNames[index] = getDatabaseTopicName(databaseNameParts[0], databaseNameParts[1]);
      index++;
    }

    _kafkaConsumer.subscribe(databaseTopicNames);
  }

  static TopicPartition[] getTopicPartitions(DatabaseTablePartition... databaseTablePartitions) {
    List<TopicPartition> topicPartitions = new ArrayList<>();

    for(DatabaseTablePartition dtp : databaseTablePartitions) {
      topicPartitions.add(getTopicPartition(dtp));
    }

    return topicPartitions.toArray(new TopicPartition[topicPartitions.size()]);
  }

  public static TopicPartition getTopicPartition(DatabaseTablePartition dtp) {
    String topicName = getTableTopicName(dtp.getDatastreamName(), dtp.getDatabaseName(), dtp.getTableName());
    return new TopicPartition(topicName, dtp.getPartition());
  }

  @Override
  public Checkpoint position(DatabaseTablePartition dtp) {
    long offset = _kafkaConsumer.position(getTopicPartition(dtp));
    return new Checkpoint(dtp, offset, -1);
  }

  @Override
  public void seek(Checkpoint checkpoint) {
    _kafkaConsumer.seek(getTopicPartition(checkpoint.getDatabaseTablePartition()), checkpoint.getOffset());
  }

  @Override
  public DatastreamRecords poll(long timeout) {
    ConsumerRecords<String, byte[]> records = _kafkaConsumer.poll(timeout);
    return new DatastreamRecords(records);
  }

  @Override
  public List<PartitionInfo> getPartitionsInDatabase(String datastreamName, String databaseName, String tableName) {
    String topicName = getTableTopicName(datastreamName, databaseName, tableName);
    return _kafkaConsumer.partitionsFor(topicName);
  }

  @Override
  public void seekToEnd(DatabaseTablePartition... databaseTablePartitions) {
    _kafkaConsumer.seekToEnd(getTopicPartitions(databaseTablePartitions));
  }

  @Override
  public void startConsumingFromBootstrap(DatabaseTablePartition... databaseTablePartitions) {
    throw new NotImplementedException();
  }

  public static String getDatabaseTopicName(String datastreamName, String databaseName) {
    return String.format(DatabaseTopicNameFmt, datastreamName, databaseName);
  }

  public static String getTableTopicName(String datastreamName, String databaseName, String tableName) {
    return String.format(TableTopicNameFmt, datastreamName, databaseName, tableName);
  }

  @Override
  public void seekToBeginning(DatabaseTablePartition... databaseTablePartitions) {
    _kafkaConsumer.seekToBeginning(getTopicPartitions(databaseTablePartitions));
  }
}
