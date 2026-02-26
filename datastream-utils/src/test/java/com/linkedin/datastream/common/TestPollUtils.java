/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link PollUtils}
 */
public class TestPollUtils {
  @Test
  public void testpollSimple() {
    Assert.assertFalse(PollUtils.poll(() -> false, 10, 100));
    Assert.assertTrue(PollUtils.poll(() -> true, 10, 100));
  }

  @Test
  public void testpollValidatePollCompletionTime() {
    class MyCond implements BooleanSupplier {
      private final int _timeWaitMs;
      private final long _timeThen;

      public MyCond(int timeWaitMs) {
        _timeWaitMs = timeWaitMs;
        _timeThen = System.currentTimeMillis();
      }

      @Override
      public boolean getAsBoolean() {
        return System.currentTimeMillis() - _timeThen >= _timeWaitMs;
      }
    }

    long now1 = System.currentTimeMillis();
    MyCond mycond = new MyCond(400);
    Assert.assertTrue(PollUtils.poll(mycond, 100, 500));
    long now2 = System.currentTimeMillis();

    Assert.assertTrue(now2 - now1 >= 300);
  }

  @Test
  public void testpollWithPredicate() {
    long now1 = System.currentTimeMillis();
    boolean returnValue = PollUtils.poll((c) -> System.currentTimeMillis() >= now1 + c, 100, 600, 400);

    Assert.assertTrue(returnValue);
    long now2 = System.currentTimeMillis();
    Assert.assertTrue(now2 - now1 >= 350);
  }

  /**
   * Validates that the predicate overload uses wall-clock time for timeout.
   * The predicate simulates a slow operation (50ms per call) with a short sleep period (10ms).
   * With the old accumulator-based approach, only 10ms would count per iteration toward the
   * 500ms timeout, so it would take ~3000ms wall-clock time (500/10 * 60ms per iteration).
   * With wall-clock timeout, it should complete within ~600ms (500ms timeout + tolerance).
   */
  @Test
  public void testPredicatePollTimesOutByWallClock() {
    AtomicInteger invocationCount = new AtomicInteger(0);
    long periodMs = 10;
    long timeoutMs = 500;

    long startMs = System.currentTimeMillis();
    boolean result = PollUtils.poll((ignored) -> {
      invocationCount.incrementAndGet();
      try {
        // Simulate a slow predicate (e.g., network call) that takes 50ms each time
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return false; // Never satisfy the condition
    }, periodMs, timeoutMs, null);
    long elapsedMs = System.currentTimeMillis() - startMs;

    Assert.assertFalse(result, "Poll should return false since the condition is never met");
    // Wall-clock elapsed time should be close to the timeout, not inflated
    // With old code: elapsed would be ~invocations * (50ms + 10ms) = many seconds
    // With fix: elapsed should be approximately timeoutMs
    Assert.assertTrue(elapsedMs < timeoutMs + 200,
        "Wall-clock time (" + elapsedMs + "ms) should not significantly exceed timeout ("
            + timeoutMs + "ms). Predicate was invoked " + invocationCount.get() + " times.");
    Assert.assertTrue(elapsedMs >= timeoutMs - 100,
        "Wall-clock time (" + elapsedMs + "ms) should be close to timeout (" + timeoutMs + "ms)");
  }

  /**
   * Validates that the supplier overload uses wall-clock time for timeout.
   * The supplier simulates a slow operation (50ms per call) with a short sleep period (10ms).
   */
  @Test
  public void testSupplierPollTimesOutByWallClock() {
    AtomicInteger invocationCount = new AtomicInteger(0);
    long periodMs = 10;
    long timeoutMs = 500;

    long startMs = System.currentTimeMillis();
    Optional<String> result = PollUtils.poll(() -> {
      invocationCount.incrementAndGet();
      try {
        // Simulate a slow supplier (e.g., schema registry lookup) that takes 50ms
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return null; // Return null so the condition check below fails
    }, (val) -> val != null, periodMs, timeoutMs);
    long elapsedMs = System.currentTimeMillis() - startMs;

    Assert.assertFalse(result.isPresent(), "Poll should return empty since supplier never returns non-null");
    Assert.assertTrue(elapsedMs < timeoutMs + 200,
        "Wall-clock time (" + elapsedMs + "ms) should not significantly exceed timeout ("
            + timeoutMs + "ms). Supplier was invoked " + invocationCount.get() + " times.");
    Assert.assertTrue(elapsedMs >= timeoutMs - 100,
        "Wall-clock time (" + elapsedMs + "ms) should be close to timeout (" + timeoutMs + "ms)");
  }

  /**
   * Validates that the predicate overload still returns true promptly when the condition
   * is met, even when the predicate takes time to execute.
   */
  @Test
  public void testPredicatePollSucceedsWithSlowPredicate() {
    AtomicInteger invocationCount = new AtomicInteger(0);

    long startMs = System.currentTimeMillis();
    boolean result = PollUtils.poll((ignored) -> {
      int count = invocationCount.incrementAndGet();
      try {
        Thread.sleep(30); // Each invocation takes 30ms
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return count >= 3; // Succeed on the 3rd invocation
    }, 10, 2000, null);
    long elapsedMs = System.currentTimeMillis() - startMs;

    Assert.assertTrue(result, "Poll should succeed on the 3rd invocation");
    Assert.assertEquals(invocationCount.get(), 3);
    // 3 invocations * ~40ms each (30ms work + 10ms sleep) = ~120ms, well under 2s timeout
    Assert.assertTrue(elapsedMs < 500,
        "Should complete well before timeout. Elapsed: " + elapsedMs + "ms");
  }

  /**
   * Validates that the supplier overload still returns the result promptly when
   * the condition is met, even when the supplier takes time to execute.
   */
  @Test
  public void testSupplierPollSucceedsWithSlowSupplier() {
    AtomicInteger invocationCount = new AtomicInteger(0);

    long startMs = System.currentTimeMillis();
    Optional<String> result = PollUtils.poll(() -> {
      int count = invocationCount.incrementAndGet();
      try {
        Thread.sleep(30); // Each invocation takes 30ms
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return count >= 3 ? "success" : null;
    }, (val) -> val != null, 10, 2000);
    long elapsedMs = System.currentTimeMillis() - startMs;

    Assert.assertTrue(result.isPresent(), "Poll should return a value on the 3rd invocation");
    Assert.assertEquals(result.get(), "success");
    Assert.assertEquals(invocationCount.get(), 3);
    Assert.assertTrue(elapsedMs < 500,
        "Should complete well before timeout. Elapsed: " + elapsedMs + "ms");
  }

  /**
   * Validates that the BooleanSupplier overload (which delegates to the predicate overload)
   * also respects wall-clock timeout when the supplier is slow.
   */
  @Test
  public void testBooleanSupplierPollTimesOutByWallClock() {
    AtomicInteger invocationCount = new AtomicInteger(0);
    long periodMs = 10;
    long timeoutMs = 500;

    long startMs = System.currentTimeMillis();
    boolean result = PollUtils.poll(() -> {
      invocationCount.incrementAndGet();
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return false;
    }, periodMs, timeoutMs);
    long elapsedMs = System.currentTimeMillis() - startMs;

    Assert.assertFalse(result);
    Assert.assertTrue(elapsedMs < timeoutMs + 200,
        "Wall-clock time (" + elapsedMs + "ms) should not significantly exceed timeout ("
            + timeoutMs + "ms). Supplier was invoked " + invocationCount.get() + " times.");
  }

  /**
   * Validates that periodMs > timeoutMs still returns false/empty immediately
   * for both overloads (edge case preserved by the fix).
   */
  @Test
  public void testPeriodExceedsTimeoutReturnsImmediately() {
    long startMs = System.currentTimeMillis();

    // Predicate overload with positive timeout
    Assert.assertFalse(PollUtils.poll((ignored) -> true, 200, 100, null));

    // Supplier overload
    Optional<String> result = PollUtils.poll(() -> "value", (val) -> true, 200, 100);
    Assert.assertFalse(result.isPresent());

    long elapsedMs = System.currentTimeMillis() - startMs;
    Assert.assertTrue(elapsedMs < 50, "Should return immediately when periodMs > timeoutMs");
  }
}
