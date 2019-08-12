/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.EnumMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A blocking queue for {@link Coordinator} events
 * @see CoordinatorEvent.EventType
 */
public class CoordinatorEventBlockingQueue {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventBlockingQueue.class.getName());
  private final Map<CoordinatorEvent.EventType, CoordinatorEvent> _eventMap;
  private final Queue<CoordinatorEvent> _eventQueue;

  /**
   * Construct a blocking event queue for all types of events in {@link CoordinatorEvent.EventType}
   */
  public CoordinatorEventBlockingQueue() {
    _eventMap = new EnumMap<>(CoordinatorEvent.EventType.class);
    _eventQueue = new LinkedBlockingQueue<>();
  }

  /**
   * Add a single event to the queue, overwriting events with the same name
   * @param event CoordinatorEvent event to add to the queue
   */
  public synchronized void put(CoordinatorEvent event) {
    LOG.info("Queuing event {} to event queue", event.getType());
    if (!_eventMap.containsKey(event.getType())) {
      // only insert if there isn't an event present in the queue with the same name
      boolean result = _eventQueue.offer(event);
      if (!result) {
        return;
      }
    }

    // we put into the eventMap for dedup only if there is no metadata
    if (event.getEventMetadata() == null) {
      _eventMap.put(event.getType(), event);
    }

    LOG.debug("Event queue size %d", _eventQueue.size());
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

    if (queuedEvent != null) {
      LOG.info("De-queuing event " + queuedEvent.getType());
      LOG.debug("Event queue size: %d", _eventQueue.size());
      _eventMap.remove(queuedEvent.getType());
      return queuedEvent;
    }

    return null;
  }

  /**
   * Retrieve, but do not remove, the event at the head of this queue,
   * or return {@code null} if this queue is empty.
   *
   * @return the head of this queue, or {@code null} if this queue is empty
   */
  public synchronized CoordinatorEvent peek() {
    CoordinatorEvent queuedEvent = _eventQueue.peek();
    if (queuedEvent != null) {
      return _eventMap.get(queuedEvent.getType());
    }
    return null;
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
