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
    long startTime = System.currentTimeMillis();
    long endTime;
    CompletableFuture<Integer> result = FutureUtils.getIfDoneOrCancel(future, timeout);
    try {
      result.join();
      endTime = System.currentTimeMillis();
    } catch (Exception ex) {
      endTime = System.currentTimeMillis();
      Assert.assertTrue(ex instanceof CancellationException);
      exceptionThrown = true;
    }
    Assert.assertTrue(future.isDone());
    Assert.assertEquals(exceptionThrown, expectException);
    Assert.assertEquals(future.isCompletedExceptionally(), expectException);
    Assert.assertTrue(result.isDone());
    Assert.assertEquals(result.isCancelled(), expectException);
    if (!exceptionThrown) {
      Assert.assertEquals((Integer) 10, future.get());
      Assert.assertEquals((Integer) 10, result.get());
    } else {
      Assert.assertTrue((endTime - startTime) < futureRunTime.toMillis());
    }
  }
}