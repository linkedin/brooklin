/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.apache.commons.lang.NullArgumentException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * Utilities for working with CompletableFutures
 */
public class CompletableFutureUtils {
  private static final ScheduledExecutorService SCHEDULER = Executors.newScheduledThreadPool(1,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("failAfter-%d").build());

  /**
   * Returns a CompletableFuture which fails with a TimeoutException after the given interval
   * @param duration Duration after which to fail
   */
  public static <T> CompletableFuture<T> failAfter(Duration duration) {
    final CompletableFuture<T> promise = new CompletableFuture<>();
    SCHEDULER.schedule(() -> {
      TimeoutException ex = new TimeoutException(String.format("Timeout after {}ms", duration));
      return promise.completeExceptionally(ex);
    }, duration.toMillis(), TimeUnit.MILLISECONDS);
    return promise;
  }

  /**
   * Returns a {@link CompletableFuture} which either successfully executes the given future, or fails with timeout
   * after the given duration
   * @param future Future to execute
   * @param duration Timeout duration
   * @throws NullArgumentException
   */
  public static <T> CompletableFuture<T> within(CompletableFuture<T> future, Duration duration) throws
                                                                                                NullArgumentException {
    if (future == null) {
      throw new NullArgumentException("future");
    }

    CompletableFuture<T> timeout = failAfter(duration);
    return future.applyToEither(timeout, Function.identity());
  }

}