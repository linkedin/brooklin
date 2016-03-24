package com.linkedin.datastream.server;

import org.apache.avro.Schema;

import com.linkedin.datastream.server.api.schemaregistry.SchemaRegistryException;


/**
 * DatastreamEventProducer is the interface for Connectors to send
 * events to the designated destination. The producer also supports
 * two types of checkpoint policies: DATASTREAM or CUSTOM.
 * If a connector elects the former, the producer handles checkpoint
 * save/restore automatically behind the scene where connector only
 * need to start consuming from the loaded checkpoints. With custom
 * checkpoint, connectors are responsible for checkpoint processing
 * and are able to obtain a map of safe checkpoints, all events
 * before which are guaranteed to have been flushed to the transport.
 */
public interface DatastreamEventProducer {
  /**
   * Send event onto the transport
   * @param event
   */
  void send(DatastreamProducerRecord event);

  /**
   * Register the schema in schema registry. If the schema already exists in the registry
   * Just return the schema Id of the existing
   * @param schemaName Name of the schema. Schema within the same name needs to be backward compatible.
   * @param schema Schema that needs to be registered.
   * @return
   *   SchemaId of the registered schema.
   */
  String registerSchema(String schemaName, Schema schema) throws SchemaRegistryException;

  /**
   * Flush the transport for the pending events. This can be a slow and heavy operation.
   * As such, it is not efficient to be invoked very frequently.
   */
  void flush();
}
