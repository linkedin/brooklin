/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.callbackstatus;

import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
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

  // Hashset storing all the records which are yet to be acked
  private final Set<T> _inFlight = Collections.synchronizedSet(new LinkedHashSet<>());

  // Deque to store all the messages which are inflight after the last consumer checkpoint is made
  private final Deque<T> _inFlightAfterLastConsumerCheckpoint = new LinkedList<>();

  // Hashset storing all the records for which the ack is received
  private final Set<T> _acked = Collections.synchronizedSet(new HashSet<>());

  // the last checkpoint-ed record's offset
  protected T _currentCheckpoint = null;

  /**
   * Get the latest checkpoint to be acked
   * @return <T> Type of the comparable checkpoint object internally used by the connector.
   */
  @Override
  public T getAckCheckpoint() {
    return _currentCheckpoint;
  }

  /**
   * Get the count of the records which are in flight
   */
  @Override
  public long getInFlightCount() {
    return _inFlight.size();
  }

  /**
   * Get the count of the records which are all acked from the producer
   */
  @Override
  public long getAckMessagesPastCheckpointCount() {
    return _acked.size();
  }

  /**
   * Registers the given checkpoint by adding it to the deque of in-flight checkpoints.
   * @param checkpoint is the latest record acked by the producer of the underlying pub sub framework
   */
  @Override
  public synchronized void register(T checkpoint) {
    _inFlight.add(checkpoint);
    _inFlightAfterLastConsumerCheckpoint.offerLast(checkpoint);
  }

  /**
   * The checkpoint acknowledgement can be received out of order. So here, we track the checkpoints by adding
   * them in the _acked set and only update the _currentCheckpoint if a contiguous sequence of offsets are ack-ed
   * from the front of the queue.
   */
  @Override
  public synchronized void ack(T checkpoint) {
    // adding the checkpoint in the _acked set
    _acked.add(checkpoint);

    // removing the checkpoint from the _inFlight set as we got acknowledgement for this checkpoint from producer
    _inFlight.remove(checkpoint);

    // Until a contiguous sequence of offsets are not ack-ed from the producer for all the consumed records, we can't
    // commit new checkpoint to consumer. This loops checks for that contiguous acked offsets.
    while (!_inFlightAfterLastConsumerCheckpoint.isEmpty() && !_acked.isEmpty() && _acked.contains(
        _inFlightAfterLastConsumerCheckpoint.peekFirst())) {
      _currentCheckpoint = _inFlightAfterLastConsumerCheckpoint.pollFirst();

      if (!_acked.remove(_currentCheckpoint)) {
        LOG.error("Internal state error; could not remove checkpoint {}", _currentCheckpoint);
      }
    }
  }
}