package com.linkedin.datastream.server;

import java.util.Map;

import org.apache.avro.Schema;

/**
 * DatastreamEventProducer is the interface for Connectors to send
 * events to the designated destination. The producer also supports
 * two types of checkpoint policies: DATASTREAM or CUSTOM.
 * If connectors elect the former, the producer handles checkpoint
 * save/restore automatically behind the scene where connectors only
 * need to start consuming with the loaded checkpoints. With custom
 * checkpoint, connectors are responsible for checkpoint processing
 * and are able to obtain a map of safe checkpoints, all events
 * before which are guaranteed to have been flushed onto the transport.
 */
public interface DatastreamEventProducer {
  /**
   * Policy for checkpoint handling
   */
  enum CheckpointPolicy { DATASTREAM, CUSTOM }

  /**
   * Send event onto the transport
   * @param event
   */
  void send(DatastreamEventRecord event);

  /**
   * Register the schema in schema registry. If the schema already exists in the registry
   * Just return the schema Id of the existing
   * @param schema Schema that needs to be registered.
   * @return
   *   SchemaId of the registered schema.
   */
  String registerSchema(Schema schema);

  /**
   * @return a map of safe checkpoints which are guaranteed
   * to have been flushed onto the transport.
   */
  Map<DatastreamTask, String> getSafeCheckpoints();

  /**
   * Shutdown the producer and cleanup any resources.
   */
  void shutdown();
}
