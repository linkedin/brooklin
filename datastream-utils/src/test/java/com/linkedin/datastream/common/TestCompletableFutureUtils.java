/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang.NullArgumentException;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link CompletableFutureUtils}
 */
public class TestCompletableFutureUtils {

  @Test
  public void withinTimesOutAfterDuration() {
    CompletableFuture<String> future = CompletableFuture.supplyAsync(() -> {
      try {
        Thread.sleep(10000);
      } catch (Exception ex) {
        throw new RuntimeException();
      }
      return null;
    });

    boolean exception = false;
    try {
      CompletableFutureUtils.within(future, Duration.ofMillis(10)).join();
    } catch (Exception ex) {
      Throwable cause = ex.getCause();
      Assert.assertTrue(cause instanceof TimeoutException);
      exception = true;
    }

    Assert.assertTrue(exception);
  }

  @Test
  public void failAfterActuallyFailsAfterDuration() {
    CompletableFuture<String> future = CompletableFutureUtils.failAfter(Duration.ofMillis(10));

    boolean exception = false;
    try {
      future.join();
    } catch (Exception ex) {
      Throwable cause = ex.getCause();
      Assert.assertTrue(cause instanceof TimeoutException);
      exception = true;
    }
    Assert.assertTrue(exception);
  }

  @Test
  public void withinThrowsNullArgumentExceptionIfNoFutureProvided() {
    boolean exception = false;

    try {
      CompletableFutureUtils.within(null, Duration.ofMillis(100));
    } catch (NullArgumentException ex) {
      exception = true;
    }

    Assert.assertTrue(exception);
  }
}