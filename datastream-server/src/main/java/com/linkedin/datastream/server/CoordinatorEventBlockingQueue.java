package com.linkedin.datastream.server;

import java.util.Map;
import java.util.EnumMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CoordinatorEventBlockingQueue {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventBlockingQueue.class.getName());
  private final Map<CoordinatorEvent.EventType, CoordinatorEvent> _eventMap;
  private final Queue<CoordinatorEvent> _eventQueue;

  public CoordinatorEventBlockingQueue() {
    _eventMap = new EnumMap<>(CoordinatorEvent.EventType.class);
    _eventQueue = new LinkedBlockingQueue<>();
  }

  /**
  * Add a single event to the queue, overwriting events with the same name
  * @param event CoordinatorEvent event to add to the queue
  */
  public synchronized void put(CoordinatorEvent event) {
    LOG.info(String.format("Queuing event %s to event queue", event.getType()));
    if (!_eventMap.containsKey(event.getType())) {
      // only insert if there isn't an event present in the queue with the same name
      boolean result = _eventQueue.offer(event);
      if (!result) {
        return;
      }
    }

    // always overwrite the event in the map
    _eventMap.put(event.getType(), event);
    LOG.debug("Event queue size %d", _eventQueue.size());
    notify();
  }

  public synchronized CoordinatorEvent take() throws InterruptedException {
    while (_eventQueue.isEmpty()) {
      wait();
    }

    CoordinatorEvent queuedEvent = _eventQueue.poll();

    if (queuedEvent != null) {
      LOG.info("De-queuing event " + queuedEvent.getType());
      LOG.debug("Event queue size: %d", _eventQueue.size());
      return _eventMap.remove(queuedEvent.getType());
    }

    return null;
  }

  public synchronized CoordinatorEvent peek() {
    CoordinatorEvent queuedEvent = _eventQueue.peek();
    if (queuedEvent != null) {
      return _eventMap.get(queuedEvent.getType());
    }
    return null;
  }

  public int size() {
    return _eventQueue.size();
  }

  public boolean isEmpty() {
    return _eventQueue.isEmpty();
  }
}
