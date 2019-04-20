/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;


/**
 * Utility methods for dealing with threading-related concerns.
 */
public final class ThreadUtils {
  /**
   * Gracefully shutdown an executor service and force terminate if the worker threads
   * do not exit within the specified time out. Adapted from JDK javadoc:
   *
   * https://docs.oracle.com/javase/7/docs/api/java/util/concurrent/ExecutorService.html
   *
   * @param executor executor service to be shut down
   * @param timeout timeout for the graceful wait
   * @param logger caller's logger for proper logging
   * @return whether executor service is terminated
   */
  public static boolean shutdownExecutor(ExecutorService executor, Duration timeout, Logger logger) {
    // Disable new tasks from being submitted
    executor.shutdown();

    try {
      // Wait for existing tasks to terminate up to timeout
      if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        logger.warn("Executor service did not terminate gracefully within {}, force terminating ...", timeout);

        // Cancel currently executing tasks
        executor.shutdownNow();

        // Wait for tasks to respond to being cancelled up to timeout
        if (!executor.awaitTermination(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
          logger.error("Executor service failed to terminate (forced), worker threads might be swallowing "
              + "InterruptedException, giving up...");
        }
      }
    } catch (InterruptedException ie) {
      logger.warn("Interrupted while waiting for executor service to terminate gracefully, force terminating ...");

      // (Re-)Cancel if current thread also interrupted. It is safe to call shutdownNow multiple
      // times since it only interrupts active workers that have not been interrupted before by
      // checking the thread state.
      executor.shutdownNow();

      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    return executor.isTerminated();
  }
}
