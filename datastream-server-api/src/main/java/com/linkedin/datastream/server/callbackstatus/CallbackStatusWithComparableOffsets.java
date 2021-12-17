/**
 *  Copyright 2021 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.callbackstatus;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Helper class to store the callback status of the inflight events with comparable offsets.
 *
 * @param <T> Type of the comparable checkpoint object internally used by the connector.
 */
public class CallbackStatusWithComparableOffsets<T extends Comparable<T>> extends CallbackStatus<T> {

  private static final Logger LOG = LoggerFactory.getLogger(CallbackStatusWithComparableOffsets.class);

  private T _highWaterMark = null;

  // the last checkpoint-ed record's offset
  protected T _currentCheckpoint = null;

  private final Queue<T> _acked = new PriorityQueue<>();
  private final Set<T> _inFlight = Collections.synchronizedSet(new LinkedHashSet<>());

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
   * Registers the given checkpoint by adding it to the set of in-flight checkpoints.
   * @param checkpoint the checkpoint to register
   */
  public synchronized void register(T checkpoint) {
    _inFlight.add(checkpoint);
  }

  /**
   * The checkpoint acknowledgement can be received out of order. In that case we need to keep track
   * of the high watermark, and only update the ackCheckpoint when we are sure all events before it has
   * been received.
   */
  public synchronized void ack(T checkpoint) {
    if (!_inFlight.remove(checkpoint)) {
      LOG.error("Internal state error; could not remove checkpoint {}", checkpoint);
    }
    _acked.add(checkpoint);

    if (_highWaterMark == null || _highWaterMark.compareTo(checkpoint) < 0) {
      _highWaterMark = checkpoint;
    }

    if (_inFlight.isEmpty()) {
      // Queue is empty, update to high water mark.
      _currentCheckpoint = _highWaterMark;
      _acked.clear();
    } else {
      // Update the checkpoint to the largest acked message that is still smaller than the first in-flight message
      T max = null;
      T first = _inFlight.iterator().next();
      while (!_acked.isEmpty() && _acked.peek().compareTo(first) < 0) {
        max = _acked.poll();
      }
      if (max != null) {
        if (_currentCheckpoint != null && max.compareTo(_currentCheckpoint) < 0) {
          // max is less than current checkpoint, should not happen
          LOG.error(
              "Internal error: checkpoints should progress in increasing order. Resolved checkpoint as {} which is "
                  + "less than current checkpoint of {}",
              max, _currentCheckpoint);
        }
        _currentCheckpoint = max;
      }
    }
  }
}