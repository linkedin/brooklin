package com.linkedin.datastream.common;

import java.util.concurrent.CancellationException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.Validate;


/**
 * A {@link Future} impl that wraps a {@link Thread}.
 */
public class ThreadFuture implements Future<Void> {
  private final Thread _thread;

  // Indicates whether cancel() was called on this Future
  private volatile boolean _isCancelInvoked;
  // Indicates whether the Future has been cancelled successfully
  private volatile boolean _isCancelled;

  public ThreadFuture(Thread thread) {
    Validate.notNull(thread);
    _thread = thread;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    _isCancelInvoked = true;

    /*
     * Future.cancel() javadocs:
     *    Calling cancel() will fail if the task has already completed, has already been cancelled,
     *    or could not be cancelled for some other reason.
     */
    if (!_thread.isAlive() || _isCancelled) {
      return false;
    }

    /*
     * Future.cancel() javadocs:
     *    If the task has already started, then the mayInterruptIfRunning parameter determines whether
     *    the thread executing this task should be interrupted in an attempt to stop the task.
     */
    if (mayInterruptIfRunning) {
      _thread.interrupt();
    }

    _isCancelled = true;
    return true;
  }

  @Override
  public boolean isCancelled() {
    /*
     * Future.cancel() javadocs:
     *    Subsequent calls to isCancelled will always return true if Future.cancel() returned true.
     */
    return _isCancelled;
  }

  @Override
  public boolean isDone() {
    /*
     * Future.cancel() javadocs:
     *    After Future.cancel() returns, subsequent calls to isDone will always return true.
     *
     * Future.isDone() javadocs:
     *    Returns true if this task completed. Completion may be due to normal termination,
     *    an exception, or cancellation -- in all of these cases, this method will return true.
     */
    return _isCancelInvoked || !_thread.isAlive();
  }

  @Override
  public Void get() throws InterruptedException {
    /*
     * Future.get() javadocs:
     *    Throws: CancellationException – if the computation was cancelled
     */
    throwIfCancelled();
    _thread.join();
    return null;
  }

  @Override
  public Void get(long timeout, TimeUnit unit) throws InterruptedException {
    throwIfCancelled();
    _thread.join(unit.toMillis(timeout));
    return null;
  }

  private void throwIfCancelled() {
    if (isCancelled()) {
      throw new CancellationException();
    }
  }
}
