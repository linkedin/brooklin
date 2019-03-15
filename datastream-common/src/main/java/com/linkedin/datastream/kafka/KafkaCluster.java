/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

/**
 * Interface to control the Kafka cluster that is setup during testing.
 */
public interface KafkaCluster {

  /**
   * @return the brokers that are part of the Kafka cluster.
   */
  String getBrokers();

  /**
   * @return ZooKeeper connection string used by the Kafka cluster.
   */
  String getZkConnection();

  /**
   * @return whether the Kafka cluster is started or not.
   */
  boolean isStarted();

  /**
   * Start the Kafka cluster.
   */
  void startup();

  /**
   * Stop the Kafka cluster.
   */
  void shutdown();
}
