/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link FutureUtils}
 */
public class TestFutureUtils {

  @Test
  public void withinFutureTimesOutAfterDuration() {
    runAndVerifyTimeoutFuture(10000, 10, true);
    runAndVerifyTimeoutFuture(5, 10, false);
  }

  private void runAndVerifyTimeoutFuture(long futureRunTime, long timeout, boolean expectedException) {
    CompletableFuture<?> future = CompletableFuture.runAsync(() -> {
      try {
        Thread.sleep(futureRunTime);
      } catch (Exception ex) {
        throw new RuntimeException();
      }
    });

    boolean exception = false;
    CompletableFuture<?> result = FutureUtils.awaitFutureOrCancel(future, Duration.ofMillis(timeout));
    result.join();
    try {
       future.join();
    } catch (Exception ex) {
      Assert.assertTrue(ex instanceof CancellationException);
      exception = true;
    }
    Assert.assertEquals(exception, expectedException);
  }
}