package com.linkedin.datastream.server;

/**
 * Interface for Connector to send Datastream events over Kafka.
 *
 * NOTE: currently, each collector is expected to define a constructor
 * in the form of (Datastream datastream, VerifiableProperties config),
 * please refer to {@link DatastreamEventCollectorFactory}.
 */
public interface DatastreamEventCollector {
  /**
   * Send a DatastreamEvent on a Kafka cluster
   * @param record datastream event envelope
   */
  void send(DatastreamEventRecord record);
}
