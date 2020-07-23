/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
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

import org.apache.commons.lang.NullArgumentException;

import com.google.common.util.concurrent.ThreadFactoryBuilder;


/**
 * Utilities to work with {@link Future}
 */
public class FutureUtils {

  private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(4,
      new ThreadFactoryBuilder().setDaemon(true).setNameFormat("FutureUtils-%d").build());

  private FutureUtils() {

  }
  /**
   * Check if {@code future} is done after {@code after}. Otherwise, cancels {@code future}.
   * It returns {@link CompletableFuture} which should be used directly for light-weight operations to avoid blocking the
   * executor thread, or call {@link CompletableFuture#supplyAsync(Supplier)} or {@link CompletableFuture#supplyAsync(Supplier, Executor)}
   * which uses another thread to execute and does not block the main executor thread.
   * @param future {@code future} to check/cancel
   * @param after time to wait before aborting {@code future}
   * @return {@link CompletableFuture} completed with computed result of {@code future}, if {@code future} was done before {@code after}.
   *         {@link CompletableFuture} completed exceptionally with {@link InterruptedException}, if {@code future} was interrupted.
   *         {@link CompletableFuture} completed exceptionally with cause of Exception, if {@code future} hit {@link ExecutionException}.
   *         {@link CompletableFuture} cancelled, if {@code future} thread was cancelled.
   * @throws NullArgumentException {@link NullArgumentException} is Thrown if future is Null
   */
  public static <T> CompletableFuture<T> getIfDoneOrCancel(Future<T> future, Duration after) throws
                                                                                             NullArgumentException {
    if (future == null) {
      throw new NullArgumentException("future");
    }

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