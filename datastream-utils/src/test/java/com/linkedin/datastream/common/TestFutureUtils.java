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
public class TestFutureUtils {

  @Test
  public void withinFutureTimesOutAfterDuration() throws ExecutionException, InterruptedException {
    runAndVerifyTimeoutFuture(10000, 10, true);
    runAndVerifyTimeoutFuture(5, 10, false);
  }

  private void runAndVerifyTimeoutFuture(long futureRunTime, long timeout, boolean expectedException)
      throws ExecutionException, InterruptedException {
    CompletableFuture<Integer> future = CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(futureRunTime);
        return 10;
      } catch (Exception ex) {
        throw new RuntimeException();
      }
    });

    boolean exception = false;
    FutureUtils.getIfDoneOrCancel(future, Duration.ofMillis(timeout));
    try {
      future.join();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof CancellationException);
      exception = true;
    }
    Assert.assertEquals(exception, expectedException);
    if (!exception) {
      Assert.assertEquals((Integer) 10, future.get());
    }
  }
}