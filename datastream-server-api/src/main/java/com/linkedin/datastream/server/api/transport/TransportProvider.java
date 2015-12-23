package com.linkedin.datastream.server.api.transport;

import java.util.Properties;

import com.linkedin.datastream.server.DatastreamEventRecord;


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
   * @return Destination uri for the topic that is created.
   * @throws TransportException if the topic creation fails.
   */
  String createTopic(String topicName, int numberOfPartitions, Properties topicConfig) throws TransportException;

  /**
   * Drop the topic with the topic name
   * @param destination Destination uri.
   * @throws TransportException if the topic deletion fails.
   */
  void dropTopic(String destination) throws TransportException;

  /**
   * Send the DatastreamEvent to the topic.
   * @param record DatastreamEvent that needs to be sent to the stream.
   * @throws TransportException if the send fails.
   */
  void send(DatastreamEventRecord record) throws TransportException;

  /**
   * Closes the transport provider and its corresponding producer.
   * @throws TransportException if the close fails.
   */
  void close() throws TransportException;

  /**
   * Flush to make sure that the current set of events that are in the buffer gets flushed to the server.
   * @throws TransportException if the flush fails.
   */
  void flush() throws TransportException;
}
