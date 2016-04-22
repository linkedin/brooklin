package com.linkedin.datastream.common;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 *
 * @param <State>
 */
public abstract class RetryStrategy<State> {
  public enum Result {
    Complete, Retry, Abort
  }

  protected ScheduledExecutorService _timeoutScheduler;

  public RetryStrategy(ScheduledExecutorService timeoutScheduler) {
    _timeoutScheduler = timeoutScheduler;
  }

  private <T> Runnable getRunnable(String description, CompletableFuture<T> promise, Supplier<T> task,
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
          promise.completeExceptionally(
              new Exception(String.format("Aborting [%s] with retry strategy %s.", description, toString()), throwable));
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
              promise.completeExceptionally(new Exception(
                  String.format("Ending retries for [%s] with retry strategy %s.", description, toString())));
            }
          }
          break;
        case Abort:
        default:
          promise.completeExceptionally(
              new Exception(String.format("Aborting [%s] with retry strategy %s.", description, toString())));
          break;
      }
    };
  }

  abstract protected Duration backoff(State state);

  abstract protected boolean shouldContinue(State state);

  abstract protected State initialState();

  abstract protected State nextState(State currentState);

  public CompletableFuture<Boolean> retryPredicate(String description, Supplier<Boolean> task) {
    return retry(description, task, success -> success ? Result.Complete : Result.Retry, throwable -> Result.Retry);
  }

  public <T> CompletableFuture<T> retry(String description, Supplier<T> task, Function<T, Result> evaluate,
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


