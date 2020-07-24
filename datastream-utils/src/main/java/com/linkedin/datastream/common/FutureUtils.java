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
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Validate;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * Utilities to work with {@link Future}
 */
public class FutureUtils {

  private static final int EXECUTOR_CORE_POOL_SIZE = 4;
  private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(
      EXECUTOR_CORE_POOL_SIZE, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("FutureUtils-%d").build());

  private FutureUtils() {

  }
  /**
   * Check if {@code future} was done after the specified {@code after} elapsed. Otherwise, cancels {@code future}.
   * It returns {@link CompletableFuture} which should be used directly for lightweight operations to avoid blocking the
   * executor thread, or call {@link CompletableFuture#supplyAsync(Supplier)} or {@link CompletableFuture#supplyAsync(Supplier, Executor)}
   * which uses another thread to execute and does not block the main executor thread.
   * @param future {@code future} to check/cancel
   * @param after time to wait before aborting {@code future}
   * @return {@link CompletableFuture} completed with computed result of {@code future}, if {@code future} was done after {@code after} elapsed.
   *         {@link CompletableFuture} completed exceptionally with {@link InterruptedException}, if the thread retrieve the
   *         result of the {@code future} was interrupted.
   *         {@link CompletableFuture} completed exceptionally with the exception thrown by {@code future}'s computation if it threw one.
   *         {@link CompletableFuture} cancelled, if {@code future}'s computation was cancelled before it completed normally.
   * @throws NullPointerException {@link NullPointerException} is Thrown if future is Null
   */
  public static <T> CompletableFuture<T> getIfDoneOrCancel(Future<T> future, Duration after) {
    Validate.notNull(future);

    CompletableFuture<T> result = new CompletableFuture<>();

    SCHEDULED_EXECUTOR_SERVICE.schedule(() -> {
      if (future.isDone()) {
        try {
          T t = future.get();
          result.complete(t);
        } catch (InterruptedException e) {
          result.completeExceptionally(e);
        } catch (ExecutionException e) {
          result.completeExceptionally(e.getCause());
        } catch (CancellationException e) {
          result.cancel(false);
        }
      } else {
        future.cancel(true);
        result.cancel(false);
      }
    }, after.toMillis(), TimeUnit.MILLISECONDS);
    return result;
  }
}