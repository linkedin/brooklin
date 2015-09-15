package com.linkedin.datastream.server;

/**
 * DatastreamEventCollector is the interface for Connector to send
 * Datastream events on the Datastream Kafka cluster.
 */
public interface DatastreamEventCollector {
  void send(DatastreamEventRecord record);
}
