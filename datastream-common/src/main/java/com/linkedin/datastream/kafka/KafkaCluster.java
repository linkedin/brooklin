/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

/**
 * Interface to control the kafka cluster that is setup during testing.
 */
public interface KafkaCluster {

  /**
   * @return the brokers that are part of the kafka cluster.
   */
  String getBrokers();

  /**
   * @return the zookeeper connection string used by the kafka cluster.
   */
  String getZkConnection();

  /**
   * @return whether the kafka cluster is started or not.
   */
  boolean isStarted();

  /**
   * Start the kafka cluster.
   */
  void startup();

  /**
   * Stop the kafka cluster.
   */
  void shutdown();
}
