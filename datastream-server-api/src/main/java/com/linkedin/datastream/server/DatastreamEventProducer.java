package com.linkedin.datastream.server;

import java.util.Map;

import com.linkedin.datastream.server.api.transport.TransportException;


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
  void send(DatastreamEventRecord event)
      throws TransportException;

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
