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

import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.common.PartitionInfo;

public interface DatastreamConsumer {
  void commit(Collection<Checkpoint> checkpoint, CommitType commitType);

  void commit(CommitType commitType);

  void subscribe(DatabaseTablePartition... databaseTablePartition);

  void subscribeToTables(String... tables);

  void subscribeToDatabases(String... databases);

  Checkpoint position(DatabaseTablePartition topicPartition);

  void seek(Checkpoint checkpoint);

  DatastreamRecords poll(long timeout);

  List<PartitionInfo> getPartitionsInDatabase(String espressoCluster, String databaseName, String tableName);

  void seekToEnd(DatabaseTablePartition... databaseTablePartitions);

  void startConsumingFromBootstrap(DatabaseTablePartition... databaseTablePartitions);

  void seekToBeginning(DatabaseTablePartition... databaseTablePartitions);
}
