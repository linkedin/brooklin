/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to store the callback status of the inflight events with non comparable offsets.
 *
 * @param <T> Type of the non comparable checkpoint object internally used by the connector.
 */
public class CallbackStatusWithNonComparableOffsets<T> extends CallbackStatus<T> {

  private static final Logger LOG = LoggerFactory.getLogger(CallbackStatusWithNonComparableOffsets.class);

  // the last checkpoint-ed record's offset
  protected T _currentCheckpoint = null;

  // Deque to store all the messages which are inflight
  private final Deque<T> _inFlight = new ArrayDeque<>();

  // Hashset storing all the records for which the ack is received
  private final Set<T> _acked = Collections.synchronizedSet(new HashSet<>());

  public T getAckCheckpoint() {
    return _currentCheckpoint;
  }

  public long getInFlightCount() {
    return _inFlight.size();
  }

  public long getAckMessagesPastCheckpointCount() {
    return _acked.size();
  }

  /**
   * Registers the given checkpoint by adding it to the deque of in-flight checkpoints.
   * @param checkpoint the checkpoint to register
   */
  public synchronized void register(T checkpoint) {
    _inFlight.offerLast(checkpoint);
  }

  /**
   * The checkpoint acknowledgement can be received out of order. So here, we track the checkpoints by adding
   * them in the _acked set and only update the _currentCheckpoint if a contiguous sequence of offsets are ack-ed
   * from the front of the queue.
   */
  public synchronized void ack(T checkpoint) {
    _acked.add(checkpoint);

    while (!_inFlight.isEmpty() && !_acked.isEmpty() && _acked.contains(_inFlight.peekFirst())) {
      _currentCheckpoint = _inFlight.pollFirst();

      if (!_acked.remove(_currentCheckpoint)) {
        LOG.error("Internal state error; could not remove checkpoint {}", _currentCheckpoint);
      }
    }
  }
}