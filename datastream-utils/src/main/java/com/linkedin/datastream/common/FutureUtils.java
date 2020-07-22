/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;


public class FutureUtils {

  private static final ScheduledExecutorService SCHEDULED_EXECUTOR_SERVICE = Executors.newScheduledThreadPool(100);

  /**
   * This function waits till timeout for Future to complete, otherwise, it cancels it.
   * @param futureToAwait Future to complete/stop
   * @param timeout time to wait before aborting future
   * @return CompletableFuture for this operation
   */
  public static CompletableFuture<?> awaitFutureOrCancel(Future<?> futureToAwait, Duration timeout) {
    CompletableFuture<?> result = new CompletableFuture<>();

    SCHEDULED_EXECUTOR_SERVICE.schedule(() -> {
      if ((futureToAwait == null) || futureToAwait.isDone()) {
        result.complete(null);
      } else {
        boolean cancel = futureToAwait.cancel(true);
        if (!cancel) {
          result.cancel(true);
        } else {
          result.complete(null);
        }
      }
    }, timeout.toMillis(), TimeUnit.MILLISECONDS);
    return result;
  }
}