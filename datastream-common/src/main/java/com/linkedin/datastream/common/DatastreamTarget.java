package com.linkedin.datastream.common;

/**
 * This class mirrors the "target" field of Datastream model and used by
 * Connector to provide Kafka topic information to Coordinator.
 */
public class DatastreamTarget {
  /**
   * Name of the Kafka topic
   */
  private final String _topicName;

  /**
   * Number of partitions of the topic
   */
  private final int _partitions;

  /**
   * A comma-separated list of Kafka metadata brokers
   */
  private final String _metadataBrokers;

  public DatastreamTarget(String topicName, int partitions, String brokers) {
    this(topicName, partitions, brokers, false);
  }

  public DatastreamTarget(String topicName, int partitions, String brokers, boolean update) {
    _topicName = topicName;
    _partitions = partitions;
    _metadataBrokers = brokers;
  }

  /**
   * @return the Kafka topic name
   */
  public String getTopicName() {
    return _topicName;
  }

  /**
   * @return the number of partitions
   */
  public int getPartitions() {
    return _partitions;
  }

  /**
   * @return the comma-separated list of brokers
   */
  public String getMetadataBrokers() {
    return _metadataBrokers;
  }
}
