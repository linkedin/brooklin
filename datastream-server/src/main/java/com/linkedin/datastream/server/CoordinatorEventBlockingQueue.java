/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.metrics.BrooklinCounterInfo;
import com.linkedin.datastream.metrics.BrooklinGaugeInfo;
import com.linkedin.datastream.metrics.BrooklinMetricInfo;
import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.metrics.MetricsAware;


/**
 * A blocking queue for {@link Coordinator} events. Includes two metrics, a
 * {@link Gauge} and {@link Counter}. The gauge provides the queue size and
 * the counter increments when duplicate events are {@code put()}.
 *
 * @see CoordinatorEvent.EventType
 */
class CoordinatorEventBlockingQueue implements MetricsAware {

  private static final Logger LOG = LoggerFactory.getLogger(CoordinatorEventBlockingQueue.class.getName());
  private final Set<BrooklinMetricInfo> METRIC_INFOS = ConcurrentHashMap.newKeySet();

  static final String COUNTER_KEY = "duplicateEvents";
  static final String GAUGE_KEY = "queuedEvents";

  private final Set<CoordinatorEvent> _eventSet;
  private final Queue<CoordinatorEvent> _eventQueue;
  private final DynamicMetricsManager _dynamicMetricsManager;
  private final Gauge<Integer> _gauge;
  private final Counter _counter;

  /**
   * Construct a blocking event queue for all types of events in {@link CoordinatorEvent.EventType}
   *
   * @param key String used to register CoordinatorEventBlockQueue metrics. The metrics
   *            will be registered to {@code CoordinatorEventBlockingQueue.<key>.<metric>}.
   *            Where {@code <metric>} is either {@link CoordinatorEventBlockingQueue#COUNTER_KEY}
   *            or {@link CoordinatorEventBlockingQueue#GAUGE_KEY}.
   */
  CoordinatorEventBlockingQueue(String key) {
    _eventSet = new HashSet<>();
    _eventQueue = new LinkedBlockingQueue<>();
    _dynamicMetricsManager = DynamicMetricsManager.getInstance();

    String prefix = buildMetricName(key);
    _counter = _dynamicMetricsManager.registerMetric(prefix, COUNTER_KEY, Counter.class);
    _gauge = _dynamicMetricsManager.registerGauge(prefix, GAUGE_KEY, _eventQueue::size);

    BrooklinCounterInfo counterInfo = new BrooklinCounterInfo(MetricRegistry.name(prefix, COUNTER_KEY));
    BrooklinGaugeInfo gaugeInfo = new BrooklinGaugeInfo(MetricRegistry.name(prefix, GAUGE_KEY));
    METRIC_INFOS.addAll(Arrays.asList(counterInfo, gaugeInfo));
  }


  /**
   * Add a single event to the queue, overwriting events with the same name and same metadata.
   * @param event CoordinatorEvent event to add to the queue
   */
  public synchronized void put(CoordinatorEvent event) {
    LOG.info("Queuing event {} to event queue", event.getType());
    if (_eventSet.contains(event)) {
      _counter.inc(); // count duplicate event
    } else {
      // only insert if there isn't an event present in the queue with the same name and same metadata.
      boolean result = _eventQueue.offer(event);
      if (!result) {
        return;
      }
      _eventSet.add(event);
      _dynamicMetricsManager.setGauge(_gauge, _eventQueue::size);
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
      _dynamicMetricsManager.setGauge(_gauge, _eventQueue::size);
    }

    return queuedEvent;
  }

  /**
   *
   */
  public synchronized void clear() {
    _eventQueue.clear();
    _eventSet.clear();
    _dynamicMetricsManager.setGauge(_gauge, _eventQueue::size);
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

  @Override
  public List<BrooklinMetricInfo> getMetricInfos() {
    return new ArrayList<>(METRIC_INFOS);
  }
}
