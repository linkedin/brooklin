/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 *
 */
public class ShutdownTaskHandler {

  // Executor that handles shutting down connector consumer threads
  private ExecutorService _shutdownExecutorService = null;

  private static final Logger LOG = LoggerFactory.getLogger(ShutdownTaskHandler.class);
  /*
   * CANCEL_TASK_TIMEOUT is used to cancel a task within 5 seconds.
   * Currently the default flush timeout in Kafka producer is INT_MAX. So, if the thread is stuck during flush
   * during normal shutdown, it holds the producer lock till INT_MAX. All the subsequent tasks that are using the
   * same producer will be stuck waiting to acquire the lock. The timeout will ensure that no task holds the lock
   * more than 5 seconds during shutdown, resulting in other tasks not stuck.
   * This will limit the task as it may not get sufficient time to commit the checkpoint. The next consumer will seek
   * to the last known slightly older checkpoint and the connector will produce some duplicate events.
   */
  private static final Duration CANCEL_TASK_TIMEOUT = Duration.ofSeconds(5);

  private final Duration _shutdownTimeout;

  /**
   * @param shutdownTimeout timeout to shutdown the executor service
   */
  public ShutdownTaskHandler(Duration shutdownTimeout) {
    _shutdownTimeout = shutdownTimeout;
  }
  /**
   * Method to start the connector.
   * This is called immediately after the connector is instantiated. This typically happens when brooklin server starts up.
   */
  public void start() {
    if (_shutdownExecutorService == null) {
      _shutdownExecutorService = Executors.newCachedThreadPool();
    }
  }

  /**
   * Method to stop the connector. This is called when the brooklin server is being stopped.
   */
  public void stop() {
    if (_shutdownExecutorService != null) {
      LOG.info("Start to shut down shutdown executor and wait up to {} ms.", _shutdownTimeout.toMillis());
      ThreadUtils.shutdownExecutor(_shutdownExecutorService, _shutdownTimeout, LOG);
    }
    _shutdownExecutorService = null;
  }

  /**
   *
   * @param taskFuture Future representing pending completion of the task
   */
  public void verifyStopOrCancelTaskFuture(Future<?> taskFuture) {
    if (taskFuture != null && !taskFuture.isDone()) {
      LOG.info("Force to shutdown the consumer thread. Cancel timeout : {}", CANCEL_TASK_TIMEOUT);
      try {
        taskFuture.get(CANCEL_TASK_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
      } catch (TimeoutException | ExecutionException | InterruptedException e) {
        LOG.warn("Got exception when shutting down", e);
        if (!taskFuture.isDone()) {
          LOG.warn("Consumer is still running. Cancel the thread.");
          boolean isCanceled = taskFuture.cancel(true);
          if (!isCanceled) {
            LOG.warn("Canceling consumer returned false. Consumer may have already stopped, or cancellation failed.");
          }
        }
      }
    }
  }

  /**
   *
   * @param task the task to submit
   * @param <T> the type of the task's result
   * @return a Future representing pending completion of the task
   */
  public <T> Future<T> submit(Callable<T> task) {
    return _shutdownExecutorService.submit(task);
  }
}

