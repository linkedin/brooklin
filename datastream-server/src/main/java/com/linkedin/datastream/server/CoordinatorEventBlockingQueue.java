/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A blocking queue for {@link Coordinator} events
 * @see CoordinatorEvent.EventType
 */
public class CoordinatorEventBlockingQueue {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventBlockingQueue.class.getName());
  private final Set<CoordinatorEvent> _eventSet;
  private final Queue<CoordinatorEvent> _eventQueue;

  /**
   * Construct a blocking event queue for all types of events in {@link CoordinatorEvent.EventType}
   */
  public CoordinatorEventBlockingQueue() {
    _eventSet = new HashSet<>();
    _eventQueue = new LinkedBlockingQueue<>();
  }

  /**
   * Add a single event to the queue, overwriting events with the same name and same metadata.
   * @param event CoordinatorEvent event to add to the queue
   */
  public synchronized void put(CoordinatorEvent event) {
    LOG.info("Queuing event {} to event queue", event.getType());
    if (!_eventSet.contains(event)) {
      // only insert if there isn't an event present in the queue with the same name and same metadata.
      boolean result = _eventQueue.offer(event);
      if (!result) {
        return;
      }
      _eventSet.add(event);
    }
    LOG.debug("Event queue size {}", _eventQueue.size());
    notify();
  }

  /**
   * Retrieve and remove the event at the head of this queue, if any, or wait
   * until one is present.
   *
   * @return the event at the head of this queue
   * @throws InterruptedException if any thread interrupted the
   *             current thread before or while the current thread
   *             was waiting for a notification
   */
  public synchronized CoordinatorEvent take() throws InterruptedException {
    while (_eventQueue.isEmpty()) {
      wait();
    }

    CoordinatorEvent queuedEvent = _eventQueue.poll();

    if (CoordinatorEvent.NO_OP_EVENT == queuedEvent) {
      return null;
    }

    if (queuedEvent != null) {
      LOG.info("De-queuing event " + queuedEvent.getType());
      LOG.debug("Event queue size: {}", _eventQueue.size());
      _eventSet.remove(queuedEvent);
    }

    return queuedEvent;
  }

  /**
   *
   */
  public synchronized void clear() {
    _eventQueue.clear();
    _eventSet.clear();
  }

  /**
   * Retrieve, but do not remove, the event at the head of this queue,
   * or return {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  public synchronized CoordinatorEvent peek() {
    return _eventQueue.peek();
  }

  /**
   * Get the number of events in this queue
   */
  public int size() {
    return _eventQueue.size();
  }

  /**
   * Check if this queue is empty
   *
   * @return true if the queue is empty
   */
  public boolean isEmpty() {
    return _eventQueue.isEmpty();
  }
}
