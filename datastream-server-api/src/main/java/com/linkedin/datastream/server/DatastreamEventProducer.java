package com.linkedin.datastream.server;

import com.linkedin.datastream.server.api.transport.SendCallback;


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
  void send(DatastreamProducerRecord event, SendCallback callback);

  /**
   * Flush the transport for the pending events. This can be a slow and heavy operation.
   * As such, it is not efficient to be invoked very frequently.
   */
  void flush();
}
