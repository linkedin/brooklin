/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.testutil.MetricsTestUtils;

import static com.linkedin.datastream.server.CoordinatorEventBlockingQueue.*;


/**
 * Tests for {@link CoordinatorEventBlockingQueue}
 */
public class TestCoordinatorEventBlockingQueue {

  static {
    DynamicMetricsManager.createInstance(new MetricRegistry(), TestCoordinatorEventBlockingQueue.class.getSimpleName());
  }

  @Test
  public void testHappyPath() throws Exception {
    CoordinatorEventBlockingQueue eventBlockingQueue = new CoordinatorEventBlockingQueue();
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test2"));
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    Assert.assertEquals(eventBlockingQueue.size(), 5);
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderPartitionAssignmentEvent("test2"));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
  }

  /**
   * Verify metric registration.
   */
  @Test
  public void testRegistersMetricsCorrectly() {
    CoordinatorEventBlockingQueue queue = new CoordinatorEventBlockingQueue();
    MetricsTestUtils.verifyMetrics(queue, DynamicMetricsManager.getInstance());
  }

  /**
   * Verify gauge matches operations: {@code put()}, {@code peek()}, {@code take()},
   * and {@code clear()}.
   */
  @Test(timeOut = 100)
  public void testGaugeOperations() throws InterruptedException {
    CoordinatorEventBlockingQueue queue = new CoordinatorEventBlockingQueue();
    String metricName = queue.buildMetricName(METRIC_KEY);
    Gauge<Integer> gauge = DynamicMetricsManager.getInstance().getMetric(metricName);
    Assert.assertNotNull(gauge, "Gauge was not found. Test setup failed.");
    Assert.assertEquals((int) gauge.getValue(), 0, "Initial size should be 0.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals((int) gauge.getValue(), 1, "put() should increment gauge.");

    CoordinatorEvent event0 = queue.peek();
    Assert.assertNotNull(event0, "Event was not queued.");
    Assert.assertEquals((int) gauge.getValue(), 1, "peek() should affect gauge.");

    CoordinatorEvent event1 = queue.take();
    Assert.assertNotNull(event0, "Event was not queued.");
    Assert.assertEquals((int) gauge.getValue(), 0, "remove() should decrement gauge.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    queue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals((int) gauge.getValue(), 3 );
    queue.clear();
    Assert.assertEquals((int) gauge.getValue(), 0, "clear() should reset gauge.");
  }

  /**
   * Verify gauge follows de-duplication. Adding duplicate events should not
   * change gauge value.
   *
   * 2-step assertion:
   *   1. queue.size() == value            => verify test construction
   *   2. gauge.getValue() == queue.size() => verify implementation
   */
  @Test(timeOut = 100)
  public void testGaugeDedupe() throws InterruptedException {
    CoordinatorEventBlockingQueue queue = new CoordinatorEventBlockingQueue();
    String metricName = queue.buildMetricName(METRIC_KEY);
    Gauge<Integer> gauge = DynamicMetricsManager.getInstance().getMetric(metricName);
    Assert.assertNotNull(gauge, "Gauge was not found. Test setup failed.");
    Assert.assertEquals((int) gauge.getValue(), 0, "Initial size should be 0.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(queue.size(), 1);
    Assert.assertEquals((int) gauge.getValue(), 1, "Add should increment gauge.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(queue.size(), 1);
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Duplicate event should not change gauge.");

    queue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals(queue.size(), 2);
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Add should increment gauge.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(queue.size(), 2);
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Duplicate event should not change gauge.");

    CoordinatorEvent event0 = queue.take();
    Assert.assertNotNull(event0, "Event was not queued.");
    Assert.assertEquals(queue.size(), 1);
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Remove should decrement gauge.");

    CoordinatorEvent event1 = queue.take();
    Assert.assertNotNull(event1, "Event was not queued.");
    Assert.assertEquals(queue.size(), 0);
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Remove should decrement gauge.");

    CoordinatorEvent event2 = queue.peek();
    Assert.assertNull(event2, "Event queue was expected to be empty.");
    Assert.assertEquals(queue.size(), 0);
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Value is never less than zero.");
  }
}
