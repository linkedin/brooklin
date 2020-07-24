/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link FutureUtils}
 */
@Test
public class TestFutureUtils {

  @Test
  public void withinFutureTimesOutAfterDuration() throws ExecutionException, InterruptedException {
    runAndVerifyTimeoutFuture(Duration.ofMillis(10000), Duration.ofMillis(10), true);
    runAndVerifyTimeoutFuture(Duration.ofMillis(5), Duration.ofMillis(10), false);
  }

  private void runAndVerifyTimeoutFuture(Duration futureRunTime, Duration timeout, boolean expectException)
      throws ExecutionException, InterruptedException {
    CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(futureRunTime.toMillis());
        return 10;
      } catch (Exception ex) {
        throw new RuntimeException();
      }
    });

    boolean exceptionThrown = false;
    FutureUtils.getIfDoneOrCancel(future, timeout);
    try {
      future.join();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof CancellationException);
      exceptionThrown = true;
    }
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(exceptionThrown, expectException);
    Assert.assertEquals(future.isCompletedExceptionally(), expectException);
    if (!exceptionThrown) {
      Assert.assertEquals((Integer) 10, future.get());
    }
  }
}