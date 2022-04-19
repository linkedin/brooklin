/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
   *
   * <p>
   * Note that the onComplete callbacks will generally execute in the I/O thread of the TransportProvider and
   * should be reasonably fast or they will delay sending messages for other threads.
   * If you want to execute an expensive callbacks it is recommended to use your own
   * {@link java.util.concurrent.Executor} in the callback body to parallelize processing.
   *
   * @param event event to send
   * @param callback call back that needs to called when the send completes.
   */
  void send(DatastreamProducerRecord event, SendCallback callback);

  /**
   * Flush the transport for the pending events. This can be a slow and heavy operation.
   * As such, it is not efficient to be invoked very frequently.
   */
  void flush();

  /**
   * Enable periodic flush on send
   *
   * @param enableFlushOnSend Whether to enable flushing on send or not
   */
  default void enablePeriodicFlushOnSend(boolean enableFlushOnSend) {
  }

  /**
   * Broadcast event onto the transport. Broadcast callback.onComplete should be reasonably fast
   * for the same reason as in send.
   *
   * @param event
   * @param callback
   */
  default void broadcast(DatastreamProducerRecord event, SendCallback callback) {
    throw new UnsupportedOperationException("Broadcast not supported by event producer");
  }
}
