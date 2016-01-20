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
