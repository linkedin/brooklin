/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.callbackstatus;

/**
 * Abstract class to track the callback status of the inflight events.
 *
 * @param <T> Type of the comparable or non comparable checkpoint object internally used by the connector.
 */
abstract public class CallbackStatus<T> {

  /**
   * Get the latest checkpoint to be acked
   */
  abstract public T getAckCheckpoint();

  /**
   * Get the count of the records which are in flight
   */
  abstract public long getInFlightCount();

  /**
   * Get the count of the records which are all acked from the producer
   */
  abstract public long getAckMessagesPastCheckpointCount();

  /**
   * Registers the given checkpoint.
   * @param checkpoint is the latest record acked by the producer of the underlying pub sub framework
   */
  abstract public void register(T checkpoint);

  /**
   * The checkpoint acknowledgement maintains the last successfully checkpoint-ed entry with
   * either comparing or without comparing the offsets.
   */
  abstract public void ack(T checkpoint);
}