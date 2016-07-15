package com.linkedin.datastream.common;

/*
 * Copyright 2016 LinkedIn Corp. All rights reserved
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;


/**
 * Utility class used to perform a task asynchronously and retry it according to general retry rules as well as
 * user provided semantics.
 *
 * The need to perform task that needs to be retried happens quite often. This class makes it easy to only focus on the
 * logic of the problem at hand and leave out how many and the way retries will be queued. It is possible to implement
 * instant retries, constant time retries, retries with backoff timers and jitter and bound all of them by total time
 * or whatever logic makes sense.
 *
 * A few instances are provided useful for most cases. Users can choose to extend and implement RetryStrategy to add
 * features.
 * @param <State> When performing retries a State is carried and used to inform whether to continue or not. The State
 *               is specific to each RetryStrategy implementation. User of the class should not care and just use as
 *               {@code RetryStrategy<?>}. The State can be as simple as an Integer or an Instant or a custom class and
 *               include more fields.
 *
 * @author tlazaro
 */
public abstract class RetryStrategy<State> {
  public enum Result {
    Complete, Retry, Abort
  }

  /**
   * Specific exception class to make exceptions thrown by the retry strategy more tractable.
   */
  public static class RetryStrategyException extends Exception implements Serializable {
    static final long serialVersionUID = 1L;

    public RetryStrategyException(String message, Throwable cause) {
      super(message, cause);
    }

    public RetryStrategyException(String message) {
      this(message, null);
    }
  }

  /**
   * Represents a supplier of results which may throw exceptions.
   *
   * <p>There is no requirement that a new or distinct result be returned each
   * time the supplier is invoked.
   *
   * <p>This is a <a href="package-summary.html">functional interface</a>
   * whose functional method is {@link #get()}.
   *
   * @param <T> the type of results supplied by this supplier
   */
  @FunctionalInterface
  public interface CheckedSupplier<T> {

    /**
     * Gets a result.
     *
     * @return a result
     */
    T get() throws Exception;
  }

  protected ScheduledExecutorService _timeoutScheduler;

  public RetryStrategy(ScheduledExecutorService timeoutScheduler) {
    _timeoutScheduler = timeoutScheduler;
  }

  /**
   * Internal method used to generate the Runnable that will be scheduled to run the task. The core logic of when to
   * retry, abort or complete the final result happens here. The method looks recursive but it is not. The method
   * returns a Runnable, it is not running it yet. The apparent recursion will happen in the future, by rescheduling a
   * newly created Runnable. The state transitions of the RetryStrategy are handled here as well.
   *
   * The `run` method informs how this method should work.
   *
   * Fatal Errors will bubble up and not be handed to the user of the retry strategy. It is likely that otherwise,
   * users will swallow these exceptions and make the problem worse.
   *
   * @param description
   * @param promise
   * @param task
   * @param evaluate
   * @param handleError
   * @param state
   * @param <T>
   * @return
   */
  private <T> Runnable getRunnable(String description, CompletableFuture<T> promise, CheckedSupplier<T> task,
      Function<T, Result> evaluate, Function<Throwable, Result> handleError, State state) {
    return () -> {
      T taskResult = null;
      Result retryResult;

      // Don't perform the task at all if the promise was cancelled or completed from the outside.
      if (promise.isDone()) {
        return;
      }

      try {
        // Perform the task and capture exceptions if any
        taskResult = task.get();

        // Evaluate taskResult based on user provided semantics
        retryResult = evaluate.apply(taskResult);
      } catch (VirtualMachineError | ThreadDeath | LinkageError fatalError) {
        // Can't recover this situation, don't catch it, let it bubble up
        // `OutOfMemoryError` and `StackOverflowError` are subclasses of `VirtualMachineError`
        throw fatalError;
      } catch (Throwable throwable) {
        // Evaluate error result based on user provided error handling semantics
        retryResult = handleError.apply(throwable);

        // If not retrying any more we complete the promise with an Exception with the throwable as cause
        if (retryResult != Result.Retry) {
          // Provide the original exception coming from caller logic
          promise.completeExceptionally(throwable);
          return;
        }
      }

      switch (retryResult) {
        case Complete:
          promise.complete(taskResult);
          break;
        case Retry:
          // Stop retrying if the promise was cancelled or completed from the outside.
          if (!promise.isDone()) {
            if (shouldContinue(state)) {
              Duration back = backoff(state);
              _timeoutScheduler.schedule(
                  getRunnable(description, promise, task, evaluate, handleError, nextState(state)), back.toMillis(),
                  TimeUnit.MILLISECONDS);
            } else {
              promise.completeExceptionally(new RetryStrategyException(
                  String.format("Ending retries for [%s] with retry strategy %s.", description, toString())));
            }
          }
          break;
        case Abort:
        default:
          promise.completeExceptionally(new RetryStrategyException(
              String.format("Aborting [%s] with retry strategy %s.", description, toString())));
          break;
      }
    };
  }

  /**
   * Given the provided state, decide how much time to wait until next retry.
   * @param state
   * @return
   */
  abstract protected Duration backoff(State state);

  /**
   * Given the provided state, decide whether to retry or not. A strategy that counts attempt might abort if too many
   * attempts have been performed. A strategy keeping track of time might abort if too much time has passed.
   * @param state
   * @return
   */
  abstract protected boolean shouldContinue(State state);

  /**
   * Generate a starting state. A strategy that counts attempts will use 0. A strategy keeping track of time will
   * register the current time.
   * @return
   */
  abstract protected State initialState();

  /**
   * Given the provided state, generate the next state for this strategy. A strategy that counts attempts will increase
   * the counter by one. A strategy keeping track of time might only care about start time and not change the state.
   * @param currentState
   * @return
   */
  abstract protected State nextState(State currentState);

  /**
   * Convenience method to perform a task until it returns true, disregarding exceptions thrown by it.
   * @param description Description of the task, used in Exceptions messages arising from running this task.
   * @param task task to be run until it yields 'true' or this RetryStrategy decides to abort.
   * @return a CompletableFuture capturing either the final result asynchronous or an exception in case it aborted.
   * By construction this can only yield 'true' or an exception.
   */
  public final CompletableFuture<Boolean> until(String description, CheckedSupplier<Boolean> task) {
    return run(description, task, success -> success ? Result.Complete : Result.Retry, throwable -> Result.Retry);
  }

  /**
   * Perform a task asynchronously and retry it according to this RetryStrategy configuration and semantics following
   * the evaluation and error handling functions supplied by the caller.
   *
   * When running the task, its result will be evaluated with the provided evaluation function. In case of returning
   * Complete, the current result will be returned. In case of Retry, the task will be run again. In case of Abort, the
   * result will include an exception.
   *
   * Specific implementations of RetryStrategy will have different rules indicating when to abort the task, regardless
   * of the user evaluation function. For instance, a strategy might keep retrying when failed but abort after
   * 30 seconds have passed without success. Retries might also be delayed with a constant or backoff timer.
   *
   * <pre>{@code
   * // Retry every second up to a total of 30 seconds.
   * RetryStrategy<?> retryStrategy = new TimeoutRetryStrategy(scheduler, Duration.ofSeconds(1), Duration.ofSeconds(30));
   * CompletableFuture<String> result = retryStrategy.run("Create user",
   *                                                      () -> service.insert(new User(...)),
   *                                                      userId -> userId != null ? Complete : Retry,
   *                                                      ex -> if (ex instanceof Unauthorized) Abort : Retry);
   *
   * // Not recommended! Block until the result is produced:
   * String userId = result.get();
   * }</pre>
   *
   * Exception should contain a descriptive error
   *
   * @param description description of the task, used in Exceptions messages arising from running this task.
   * @param task task to be run at least once and as long as this RetryStrategy decides to retry it.
   * @param evaluate upon every result of the task, this function will evaluate how to proceed, whether to complete or
   *                 keep retrying or aborting.
   * @param handleError upon every result of the task yielding an Exception, this function will evaluate how to proceed,
   *                    whether to complete or keep retrying or aborting. Some APIs communicate failures as Exceptions
   *                    and might be excepted give the use-case logic of the task. A REST call might communicate
   *                    Not Found as an Exception and user might want to keep trying, while an Unauthorized is unlikely
   *                    to succeed when retried so user may choose to abort.
   * @param <T> Return type of the task which will be made available upon successful completion.
   * @return The CompletableFuture will either contain the successful result of the task or an Exception. In case
   * an Exception arising from the task occurs it will be provided as is. If the retry logic is aborted because of the
   * retry rules themselves, a RetryStrategyException will be used in the result.
   */
  public final <T> CompletableFuture<T> run(String description, CheckedSupplier<T> task, Function<T, Result> evaluate,
      Function<Throwable, Result> handleError) {
    final CompletableFuture<T> promise = new CompletableFuture<>();

    // Schedule task to run immediately
    _timeoutScheduler.schedule(getRunnable(description, promise, task, evaluate, handleError, initialState()), 0,
        TimeUnit.MILLISECONDS);

    return promise;
  }

  public static class TimeoutRetryStrategy extends RetryStrategy<Instant> {
    protected final Duration _waitTime;
    protected final Duration _maxTotalWait;

    public TimeoutRetryStrategy(ScheduledExecutorService scheduler, Duration waitTime, Duration maxTotalWait) {
      super(scheduler);
      _waitTime = waitTime;
      _maxTotalWait = maxTotalWait;
    }

    @Override
    protected Duration backoff(Instant start) {
      return _waitTime;
    }

    @Override
    protected boolean shouldContinue(Instant start) {
      return Instant.now().isBefore(start.plus(_maxTotalWait));
    }

    @Override
    protected Instant initialState() {
      return Instant.now();
    }

    @Override
    protected Instant nextState(Instant start) {
      return start;
    }

    @Override
    public String toString() {
      return "TimeoutRetryStrategy{" +
          "_waitTime=" + _waitTime +
          ", _maxTotalWait=" + _maxTotalWait +
          '}';
    }
  }

  public static class FixedTimeAttempts extends RetryStrategy<Integer> {
    protected final Duration _waitTime;
    protected final int _maxAttempts;

    public FixedTimeAttempts(ScheduledExecutorService scheduler, Duration waitTime, int maxAttempts) {
      super(scheduler);
      _waitTime = waitTime;
      _maxAttempts = maxAttempts;
    }

    @Override
    protected Duration backoff(Integer attempts) {
      return _waitTime;
    }

    @Override
    protected boolean shouldContinue(Integer attempts) {
      return attempts < _maxAttempts;
    }

    @Override
    protected Integer initialState() {
      return 0;
    }

    @Override
    protected Integer nextState(Integer attempts) {
      return attempts + 1;
    }

    @Override
    public String toString() {
      return "FixedTimeAttempts{" +
          "_waitTime=" + _waitTime +
          ", _maxAttempts=" + _maxAttempts +
          '}';
    }
  }

  public static class BackoffRetryStrategy extends FixedTimeAttempts {
    protected final float _backoffFactor;

    public BackoffRetryStrategy(ScheduledExecutorService scheduler, Duration initialSleepTime, int maxAttempts,
        float backoffFactor) {
      super(scheduler, initialSleepTime, maxAttempts);
      _backoffFactor = backoffFactor;
    }

    @Override
    protected Duration backoff(Integer attempts) {
      return _waitTime.multipliedBy((long) Math.pow(_backoffFactor, attempts));
    }

    @Override
    public String toString() {
      return "BackoffRetryStrategy{" +
          "_waitTime=" + _waitTime +
          ", _maxAttempts=" + _maxAttempts +
          ", _backoffFactor=" + _backoffFactor +
          '}';
    }
  }
}


