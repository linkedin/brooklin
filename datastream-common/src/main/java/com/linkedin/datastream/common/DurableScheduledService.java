/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import com.google.common.util.concurrent.AbstractScheduledService;
import com.google.common.util.concurrent.Service;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * This class implements a Guava-based service which runs a task periodically. This class has special durable properties
 * which make it difficult for exceptions, errors, or non-terminating code within a task to prevent future executions.
 *
 * To use this service, implement the {@link #startUp()}, {@link #shutDown()}, and {@link #runOneIteration()} methods.
 * Then, start it with {@link #startAsync()}, and stop it with {@link #stopAsync()}. For other supported APIs, see
 * {@link Service}.
 *
 * Because of its durable nature, it may be possible for the class responsible for this service to encounter errors
 * which this class is unaffected by. This may cause this service to leak. Override the {@link #hasLeaked()} method as
 * described in the comments to provide some mitigation in case this happens.
 */
public abstract class DurableScheduledService implements Service {

  private static final Logger LOG = LoggerFactory.getLogger(DurableScheduledService.class);

  /**
   * Counts instantiations of this class. Used to ensure the service and thread names are unique and monotonically
   * increasing to make debugging lifecycles easier to understand.
   */
  private static final AtomicLong CLASS_INSTANTIATION_COUNTER = new AtomicLong();
  /**
   * The default interval where the periodic task is checked for liveliness.
   */
  private static final Duration DEFAULT_WATCHER_INTERVAL = Duration.ofMinutes(1);
  /**
   * The class instantiation count of this current instantiation. Used in the service and thread names.
   */
  private final long _classInstantiationCount = CLASS_INSTANTIATION_COUNTER.incrementAndGet();
  /**
   * Counts instantiations of the periodic task. Used to ensure the service and thread name for the task are unique and
   * monotonically increasing to show lifecycle information if the periodic task gets restarted.
   */
  private final AtomicLong _taskInstantiationCount = new AtomicLong();
  /**
   * The service name.
   */
  private final String _serviceName;
  /**
   * The periodicity in which to run the periodic task.
   */
  private final Duration _runInterval;
  /**
   * The smallest duration between the last successful run of the periodic task and the current time such that something
   * is considered to be wrong with the periodic task.
   *
   * If the run time of the periodic task exceeds this duration, it will be terminated and restarted.
   */
  private final Duration _timeout;
  /**
   * The interval in which the periodic task is checked for liveliness.
   */
  private final Duration _watcherInterval;

  /**
   * The scheduled service which watches our periodic task service and ensures that it is running normally. If it is
   * not, it will be terminated and restarted.
   */
  private final AbstractScheduledService _watcherService;

  /**
   * The scheduled service to be made durable. So long as this DurableScheduledService is running, if this service
   * crashes or gets stuck, it will be restarted.
   */
  private volatile AbstractScheduledService _taskService;

  /**
   * The thread which the current periodic task is executing in, if known.
   */
  private volatile Thread _taskThread;

  /**
   * The last time that the periodic task successfully ran.
   */
  private volatile Instant _lastSuccessfulRun;

  /**
   * Creates a DurableScheduledService.
   * @param serviceName the name of the service
   * @param runInterval how frequently to run the periodic task
   * @param timeout how long between successful runs before the periodic task is considered to be unhealthy
   */
  public DurableScheduledService(final String serviceName, final Duration runInterval, final Duration timeout) {
    this(serviceName, runInterval, timeout, DEFAULT_WATCHER_INTERVAL);
  }

  /**
   * Creates a DurableScheduledService.
   * @param serviceName the name of the service
   * @param runInterval how frequently to run the periodic task
   * @param timeout how long between successful runs before the periodic task is considered to be unhealthy
   * @param watcherInterval how frequently to check on the periodic task to ensure it is healthy
   */
  public DurableScheduledService(final String serviceName, final Duration runInterval, final Duration timeout,
      final Duration watcherInterval) {
    _serviceName = serviceName;
    _runInterval = runInterval;
    _timeout = timeout;
    _watcherInterval = watcherInterval;
    _watcherService = createWatcherService();
  }

  /**
   * This method contains code that should be run before the periodic task executes. It will be called before the task
   * runs for the very first time, or when the periodic task has crashed or timed out on the previous run.
   *
   * This code must run within the watcherInterval provided to the constructor, or the task will be endlessly restarted.
   */
  protected abstract void startUp() throws Exception;

  /**
   * This method contains the task that should be performed periodically.
   */
  protected abstract void runOneIteration() throws Exception;

  /**
   * This method contains code that should be run to clean up the environment that the periodic task executes in. It
   * will be called when the task terminates, when the task has crashed, or when the task eventually halts after being
   * timed out.
   */
  protected abstract void shutDown() throws Exception;

  /**
   * If this service is handled by another thread or object that experiences an unhandled failure, then it is possible
   * this service will not get a signal to terminate. If that happens, it is almost certain that this class's threads
   * and resources will be leaked.
   *
   * To try to mitigate this risk, this method provides a hint to the service that it has leaked, which is be checked
   * periodically to see if this service should have terminated but did not do so. If at any point this method returns
   * true, the service will be terminated.
   *
   * By default, this method has no hint and will always return false. Override it as necessary.
   *
   * @return true if this service has leaked and should be terminated, false otherwise
   */
  protected boolean hasLeaked() {
    return false;
  }

  /**
   * Creates a watcher service that controls the lifecycle of a child service which runs a task periodically.
   * @return a watcher service
   */
  private AbstractScheduledService createWatcherService() {
    return new AbstractScheduledService() {
      private Instant _started; // Defined to help investigation issues (when you have a heap dump or are in a debugger)
      private Instant _stopped; // Defined to help investigation issues (when you have a heap dump or are in a debugger)

      @Override
      protected String serviceName() {
        return "taskWatcher-" + _serviceName + "-" + _classInstantiationCount;
      }

      @Override
      protected void startUp() {
        _started = Instant.now();
        _lastSuccessfulRun = Instant.now();
        LOG.info("Starting the DurableScheduledService for {}", _serviceName);
      }

      @Override
      protected void shutDown() {
        _stopped = Instant.now();
        LOG.info("Stopping the DurableScheduledService for {}", _serviceName);
        stopTask();
        if (_taskService != null && _taskService.isRunning()) {
          _taskService.stopAsync().awaitTerminated();
        }
      }

      @Override
      protected void runOneIteration() {
        try {
          // If this service has leaked, we need to terminate ourself and the periodic task.
          if (hasLeaked()) {
            LOG.warn("Detected that this service {} has leaked. Shutting the service down.", _serviceName);
            stopTask();
            stopAsync();
            return;
          }

          // If the task hasn't successfully completed in awhile, it is stuck. So, restart it.
          final Instant taskRunTimeoutTime = _lastSuccessfulRun.plus(_timeout);
          if (Instant.now().isAfter(taskRunTimeoutTime)) {
            LOG.warn("Detected that the periodic task {} has not succeeded for an extended time - "
                + "terminating and restarting it", _taskService);
            stopTask();
            startTask();
            return;
          }

          // The task might still be starting, but not timed out yet.
          if (_taskService != null && (_taskService.state() == State.NEW || _taskService.state() == State.STARTING)) {
            LOG.debug("Detected that the periodic task {} is still starting", _taskService);
            return;
          }

          // If the task isn't running (and isn't starting up), we need to restart it.
          if (_taskService == null || !_taskService.isRunning()) {
            LOG.warn("Detected that the periodic task {} is not running - starting it", _taskService);
            stopTask();
            startTask();
            return;
          }

          // No issues detected with the task
          LOG.debug("Detected no issues with the periodic task {}. Last successful run was {}.", _taskService,
              _lastSuccessfulRun);
        } catch (Exception e) {
          _stopped = Instant.now();
          LOG.error("Error encountered in the thread watching (monitoring) the periodic task {}. "
              + "The periodic task can no longer be monitored or controlled, so will terminate.", _taskService, e);
          stopTask();
          throw e;
        }
      }

      @Override
      protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, _watcherInterval.toMillis(), TimeUnit.MILLISECONDS);
      }

      private void startTask() {
        _lastSuccessfulRun = Instant.now();
        _taskThread = null;
        _taskService = createPeriodicTaskService();
        _taskService.startAsync();
        LOG.debug("Starting task {}", _taskService);
      }

      private void stopTask() {
        LOG.debug("Stopping task {}", _taskService);

        // Flag the task to be stopped
        if (_taskService != null && _taskService.isRunning()) {
          _taskService.stopAsync();
        }

        // Wait up to one second for the task to stop on its own accord
        final Duration attemptDuration = Duration.ofSeconds(1);
        try {
          _taskService.awaitTerminated(attemptDuration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
          // Best faith effort
        }

        // Make a supreme effort to kill the thread in question if it is still alive
        final Thread thread = _taskThread;
        if (thread != null && thread.isAlive()) {
          // Try to interrupt the thread a few times to encourage it to quit out
          LOG.debug("Attempting to stop thread {} by sending interrupts for task {}", thread.getName(), _taskService);
          final Duration minimumSleepDuration = Duration.ofMillis(10);
          long waitTimeMs = attemptDuration.toMillis();
          final Instant expiration = Instant.now().plus(attemptDuration);
          while (thread.isAlive() && Instant.now().isBefore(expiration)) {
            thread.interrupt();
            waitTimeMs = Math.max(waitTimeMs / 2, minimumSleepDuration.toMillis());
            try {
              Thread.sleep(waitTimeMs);
            } catch (InterruptedException ignored) {
              break; // Exit the loop immediately
            }
          }

          // Check if the thread is still alive. If so, forcibly kill it.
          if (thread.isAlive()) {
            LOG.debug("Sending interrupts failed. Attempting to forcibly kill thread {} for task {}", thread.getName(),
                _taskService);
            try {
              // Thread.stop() is deprecated as of JDK 1.2, but still implemented as of JDK 11. If this method is
              // removed in a future JDK version, then we should get rid of this clause in our unit tests.
              thread.stop();
            } catch (Exception ignored) {
              // Best faith effort
            }
          }
        }

        LOG.debug("Task {} stopped {}", _taskService,
            Optional.ofNullable(thread).map(Thread::isAlive).orElse(false) ? "unsuccessfully" : "successfully");
      }
    };
  }

  /**
   * Creates a Service that executes a task periodically.
   * @return a service that executes a task periodically
   */
  private AbstractScheduledService createPeriodicTaskService() {
    return new AbstractScheduledService() {
      private final long taskInstantiationCount = _taskInstantiationCount.incrementAndGet();
      private volatile boolean _shutDownCalled = false;
      private Instant _started; // Defined to help investigation issues (when you have a heap dump or are in a debugger)
      private Instant _stopped; // Defined to help investigation issues (when you have a heap dump or are in a debugger)

      @Override
      protected String serviceName() {
        return "task-" + _serviceName + "-" + _classInstantiationCount + "-" + taskInstantiationCount;
      }

      @Override
      protected void startUp() throws Exception {
        try {
          _started = Instant.now();
          DurableScheduledService.this.startUp();
        } catch (Exception e) {
          callShutDown();
          throw e;
        }
      }

      @Override
      protected void shutDown() throws Exception {
        callShutDown();
      }

      @Override
      protected void runOneIteration() throws Exception {
        try {
          _taskThread = Thread.currentThread();
          DurableScheduledService.this.runOneIteration();
          _lastSuccessfulRun = Instant.now();
        } catch (Exception e) {
          callShutDown();
          throw e;
        }
      }

      @Override
      protected Scheduler scheduler() {
        return Scheduler.newFixedRateSchedule(0, _runInterval.toMillis(), TimeUnit.MILLISECONDS);
      }

      private synchronized void callShutDown() throws Exception {
        if (!_shutDownCalled) {
          _shutDownCalled = true;
          _stopped = Instant.now();
          DurableScheduledService.this.shutDown();
        }
      }
    };
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "DurableScheduledService-" + _serviceName + "-" + _classInstantiationCount;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final boolean isRunning() {
    return _watcherService.isRunning();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final State state() {
    return _watcherService.state();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void addListener(@NotNull final Listener listener, @NotNull final Executor executor) {
    _watcherService.addListener(listener, executor);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final Throwable failureCause() {
    return _watcherService.failureCause();
  }

  /**
   * {@inheritDoc}
   */
  @CanIgnoreReturnValue
  @Override
  public final Service startAsync() {
    _watcherService.startAsync();
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @CanIgnoreReturnValue
  @Override
  public final Service stopAsync() {
    _watcherService.stopAsync();
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void awaitRunning() {
    _watcherService.awaitRunning();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void awaitRunning(final long timeout, @NotNull final TimeUnit unit) throws TimeoutException {
    _watcherService.awaitRunning(timeout, unit);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void awaitTerminated() {
    _watcherService.awaitTerminated();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public final void awaitTerminated(final long timeout, @NotNull final TimeUnit unit) throws TimeoutException {
    _watcherService.awaitTerminated(timeout, unit);
  }
}