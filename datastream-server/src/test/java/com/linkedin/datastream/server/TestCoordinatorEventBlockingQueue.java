/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server;

import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import com.linkedin.datastream.metrics.DynamicMetricsManager;
import com.linkedin.datastream.testutil.MetricsTestUtils;

import static com.linkedin.datastream.server.CoordinatorEventBlockingQueue.COUNTER_KEY;
import static com.linkedin.datastream.server.CoordinatorEventBlockingQueue.GAUGE_KEY;


/**
 * Tests for {@link CoordinatorEventBlockingQueue}
 */
public class TestCoordinatorEventBlockingQueue {

  private static final String SIMPLE_NAME = TestCoordinatorEventBlockingQueue.class.getSimpleName();
  private static final String COUNTER_NAME =
      MetricRegistry.name(CoordinatorEventBlockingQueue.class.getSimpleName(), SIMPLE_NAME, COUNTER_KEY);
  private static final String GAUGE_NAME =
      MetricRegistry.name(CoordinatorEventBlockingQueue.class.getSimpleName(), SIMPLE_NAME, GAUGE_KEY);

  @BeforeMethod(alwaysRun = true)
  public void resetMetrics() {
    DynamicMetricsManager.createInstance(new MetricRegistry(), TestCoordinatorEventBlockingQueue.class.getName());
  }

  @Test
  public void testHappyPath() throws Exception {
    CoordinatorEventBlockingQueue eventBlockingQueue = new CoordinatorEventBlockingQueue(SIMPLE_NAME);
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    eventBlockingQueue.putFirst(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    eventBlockingQueue.putFirst(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.putFirst(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test2"));
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.put(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    Assert.assertEquals(eventBlockingQueue.size(), 5);
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderPartitionAssignmentEvent("test2"));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
  }

  @Test
  public void testHappyPathWithDuplicatedPutFirstEventRequests() throws Exception {
    CoordinatorEventBlockingQueue eventBlockingQueue = new CoordinatorEventBlockingQueue(SIMPLE_NAME);
    eventBlockingQueue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    eventBlockingQueue.putFirst(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    eventBlockingQueue.putFirst(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    eventBlockingQueue.putFirst(CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    eventBlockingQueue.putFirst(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    Assert.assertEquals(eventBlockingQueue.size(), 4);
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.HANDLE_ASSIGNMENT_CHANGE_EVENT);
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals(eventBlockingQueue.take(), CoordinatorEvent.createLeaderDoAssignmentEvent(false));
  }

  /**
   * Verify metric registration.
   */
  @Test
  public void testRegistersMetricsCorrectly() {
    CoordinatorEventBlockingQueue queue = new CoordinatorEventBlockingQueue(SIMPLE_NAME);
    MetricsTestUtils.verifyMetrics(queue, DynamicMetricsManager.getInstance());
  }

  /**
   * Verify metrics match operations: {@code put()}, {@code peek()}, {@code take()},
   * and {@code clear()}. Counter should not be changed
   */
  @Test(timeOut = 500)
  public void testMetricOperations() throws InterruptedException {
    CoordinatorEventBlockingQueue queue = new CoordinatorEventBlockingQueue(SIMPLE_NAME);
    Counter counter = DynamicMetricsManager.getInstance().getMetric(COUNTER_NAME);
    Gauge<Integer> gauge = DynamicMetricsManager.getInstance().getMetric(GAUGE_NAME);

    Assert.assertNotNull(counter, "Counter was not found. Test setup failed.");
    Assert.assertEquals(counter.getCount(), 0, "Initial value should be 0.");

    // set counter to random negative value for verification
    int random = -new Random().nextInt();
    counter.inc(random);
    Assert.assertEquals(counter.getCount(), random, "Override was not set");

    Assert.assertNotNull(gauge, "Gauge was not found. Test setup failed.");
    Assert.assertEquals((int) gauge.getValue(), 0, "Initial value should be 0.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(counter.getCount(), random, "Adding event to empty should not increment counter.");
    Assert.assertEquals((int) gauge.getValue(), 1, "put() should increment gauge.");

    CoordinatorEvent event0 = queue.peek();
    Assert.assertNotNull(event0, "Event was not queued.");
    Assert.assertEquals(counter.getCount(), random, "peek() should not alter counter.");
    Assert.assertEquals((int) gauge.getValue(), 1, "peek() should affect gauge.");

    CoordinatorEvent event1 = queue.take();
    Assert.assertNotNull(event1, "Event was not queued.");
    Assert.assertEquals(counter.getCount(), random, "remove() should not alter counter.");
    Assert.assertEquals((int) gauge.getValue(), 0, "remove() should decrement gauge.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(true));
    queue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals((int) gauge.getValue(), 3);
    queue.clear();
    Assert.assertEquals(counter.getCount(), random, "clear() should not alter counter.");
    Assert.assertEquals((int) gauge.getValue(), 0, "clear() should reset gauge.");
  }

  /**
   * Verify counter and gauge follow de-duplication. Adding duplicate events should not
   * change gauge value, but should increment the counter.
   *
   * 2-step assertion for gauge:
   *   1. queue.size() == value            => verify test construction
   *   2. gauge.getValue() == queue.size() => verify implementation
   */
  @Test(timeOut = 500)
  public void testGaugeDedupe() throws InterruptedException {
    CoordinatorEventBlockingQueue queue = new CoordinatorEventBlockingQueue(SIMPLE_NAME);
    Counter counter = DynamicMetricsManager.getInstance().getMetric(COUNTER_NAME);
    Gauge<Integer> gauge = DynamicMetricsManager.getInstance().getMetric(GAUGE_NAME);

    Assert.assertNotNull(counter, "Counter was not found. Test setup failed.");
    Assert.assertEquals(counter.getCount(), 0, "Initial value should be 0.");

    Assert.assertNotNull(gauge, "Gauge was not found. Test setup failed.");
    Assert.assertEquals((int) gauge.getValue(), 0, "Initial value should be 0.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(queue.size(), 1);
    Assert.assertEquals(counter.getCount(), 0, "Adding event to empty should not increment counter.");
    Assert.assertEquals((int) gauge.getValue(), 1, "Add should increment gauge.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(queue.size(), 1);
    Assert.assertEquals(counter.getCount(), 1, "Failed to count duplicate event.");
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Duplicate event should not change gauge.");

    queue.put(CoordinatorEvent.createLeaderPartitionAssignmentEvent("test1"));
    Assert.assertEquals(queue.size(), 2);
    Assert.assertEquals(counter.getCount(), 1, "Counter should not have been altered.");
    Assert.assertEquals((int) gauge.getValue(), queue.size(), "Add should increment gauge.");

    queue.put(CoordinatorEvent.createLeaderDoAssignmentEvent(false));
    Assert.assertEquals(queue.size(), 2);
    Assert.assertEquals(counter.getCount(), 2, "Failed to count second duplicate event.");
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
