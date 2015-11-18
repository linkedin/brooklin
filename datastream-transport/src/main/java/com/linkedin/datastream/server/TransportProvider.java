package com.linkedin.datastream.server;

import java.util.Properties;

import com.linkedin.datastream.common.DatastreamEventRecord;


/**
 * Datastream is transport agnostic system, This is the interface that each TransportProvider needs to be implement
 * to plug the different transport mechanisms (Kafka, kinesis, etc..) to Datastream
 */
public interface TransportProvider {

  /**
   * Create a topic with specified number of configurations.
   * @param topicName  Name of the topic to be created.
   * @param numberOfPartitions Number of partitions in the topic.
   * @param topicConfig Configuration to use to create the topic.
   */
  String createTopic(String topicName, int numberOfPartitions, Properties topicConfig);

  /**
   * Drop the topic with the topic name
   * @param destination Destination uri.
   */
  void dropTopic(String destination);

  /**
   * Send the DatastreamEvent to the topic.
   * @param record DatastreamEvent that needs to be sent to the stream.
   */
  void send(DatastreamEventRecord record);

  /**
   * Flush to make sure that the current set of events that are in the buffer gets flushed to the server.
   */
  void flush();
}
