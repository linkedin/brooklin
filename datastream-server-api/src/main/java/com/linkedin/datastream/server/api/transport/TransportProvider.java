/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

import com.linkedin.datastream.server.DatastreamProducerRecord;


/**
 * Brooklin is a transport agnostic system. This is the interface that each TransportProvider needs to implement
 * to plug the different transport mechanisms (Kafka, Kinesis, etc..) to Brooklin.
 */
public interface TransportProvider {
  /**
   * Send the DatastreamEvent to the topic.
   *
   * <p>
   * Note that the onComplete callbacks will generally execute in the I/O thread of
   * the TransportProvider and should be reasonably fast or they will delay sending messages
   * for other threads. If you want to execute an expensive callbacks it is recommended to use
   * your own {@link java.util.concurrent.Executor} in the callback body to parallelize processing.
   *
   * @param destination the destination topic to which the record should be sent.
   * @param record DatastreamEvent that needs to be sent to the stream.
   * @param onComplete call back that needs to called when the send completes. Any exception during ending will
   *                   be reported through callback
   */
  void send(String destination, DatastreamProducerRecord record, SendCallback onComplete);

  /**
   * Closes the transport provider and its corresponding producer.
   */
  void close();

  /**
   * Flush to make sure that the current set of events that are in the buffer gets flushed to the server.
   */
  void flush();
}
