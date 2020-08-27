package com.linkedin.datastream.common;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.testng.Assert;
import org.testng.annotations.Test;


@Test(timeOut = 5000)
public class TestThreadFuture {

  @Test(expectedExceptions = NullPointerException.class)
  public void testCtorThrowsOnNullThread() {
    new ThreadFuture(null);
  }

  /**
   * Future.cancel() javadocs:
   * <ol>
   *   <li>
   *     Calling cancel() will fail if the task has already completed, has already been cancelled,
   *     or could not be cancelled for some other reason.</li>
   *    <li>
   *      If the task has already started, then the mayInterruptIfRunning parameter determines whether
   *      the thread executing this task should be interrupted in an attempt to stop the task.</li>
   *    <li>
   *      Subsequent calls to isCancelled will always return true if Future.cancel() returned true.</li>
   * </ol>
   */
  @Test
  public static class CancellationTests {
    private static final boolean[] TRUTH_VALUES = {true, false};

    @Test
    public void testInterruptsThreadIfDesired() throws InterruptedException {
      TestThread thread = new TestThread.Builder().build();
      Future<?> future = new ThreadFuture(thread.get());
      Assert.assertFalse(thread.isInterrupted());

      assertSuccessfulCancel(future, true);

      thread.join();
      Assert.assertTrue(thread.isInterrupted());
    }

    @Test
    public void testDoesNotInterruptThreadIfNotDesired() throws InterruptedException {
      TestThread thread = new TestThread.Builder().build();
      Future<?> future = new ThreadFuture(thread.get());
      Assert.assertFalse(thread.isInterrupted());

      assertSuccessfulCancel(future, false);

      thread.exit();
      Assert.assertFalse(thread.isInterrupted());
    }

    @Test
    public void testReturnsFalseIfThreadIsNotStarted() {
      for (boolean mayInterruptIfRunning : TRUTH_VALUES) {
        TestThread thread = new TestThread.Builder().startOnCreation(false).build();
        Future<?> future = new ThreadFuture(thread.get());

        assertFailedCancel(future, mayInterruptIfRunning);
      }
    }

    @Test
    public void testReturnsFalseIfThreadExited() throws InterruptedException {
      for (boolean throwOnExit : TRUTH_VALUES) {
        for (boolean mayInterruptIfRunning : TRUTH_VALUES) {
          TestThread thread = new TestThread.Builder().throwOnExit(throwOnExit).build();
          thread.exit();
          Future<?> future = new ThreadFuture(thread.get());

          assertFailedCancel(future, mayInterruptIfRunning);
        }
      }
    }

    @Test
    public void testReturnsFalseIfAlreadyCancelled() {
      for (boolean mayInterruptIfRunning : TRUTH_VALUES) {
        TestThread thread = new TestThread.Builder().build();
        Future<?> future = new ThreadFuture(thread.get());

        assertSuccessfulCancel(future, true); // First cancel succeeds
        assertCancel(future, mayInterruptIfRunning, false, true); // Second cancel fails
      }
    }

    private static void assertFailedCancel(Future<?> future, boolean mayInterruptIfRunning) {
      assertCancel(future, mayInterruptIfRunning, false, false);
    }

    private static void assertSuccessfulCancel(Future<?> future, boolean mayInterruptIfRunning) {
      assertCancel(future, mayInterruptIfRunning, true, true);
    }

    private static void assertCancel(Future<?> future, boolean mayInterruptIfRunning, boolean expectedResult,
        boolean expectedIsCancelled) {
      Assert.assertEquals(future.cancel(mayInterruptIfRunning), expectedResult);
      Assert.assertEquals(future.isCancelled(), expectedIsCancelled);
    }
  }

  /**
   * Future.isDone() javadocs:
   *    Returns true if this task completed. Completion may be due to normal termination,
   *    an exception, or cancellation -- in all of these cases, this method will return true.
   */
  @Test
  public static class IsDoneTests {
    @Test
    public void testReturnsTrueIfThreadIsNotAlive() throws InterruptedException {
      testReturnsTrueIfThreadIsNotAliveHelper(true);
      testReturnsTrueIfThreadIsNotAliveHelper(false);
    }

    @Test
    public void testReturnsTrueIfCancelled() {
      testReturnsTrueIfCancelledHelper(true);
      testReturnsTrueIfCancelledHelper(false);
    }

    private static void testReturnsTrueIfThreadIsNotAliveHelper(boolean throwOnExit) throws InterruptedException {
      TestThread thread = new TestThread.Builder().throwOnExit(throwOnExit).build();
      Future<?> future = new ThreadFuture(thread.get());
      Assert.assertFalse(future.isDone());

      thread.exit();
      Assert.assertTrue(future.isDone());
    }

    private static void testReturnsTrueIfCancelledHelper(boolean mayInterruptIfRunning) {
      TestThread thread = new TestThread.Builder().build();
      Future<?> future = new ThreadFuture(thread.get());
      Assert.assertFalse(future.isDone());

      future.cancel(mayInterruptIfRunning);

      Assert.assertTrue(future.isDone());
    }
  }

  @Test
  public static class GetTests {
    @Test
    public void testReturnsWhenThreadExits() throws ExecutionException, InterruptedException {
      testReturnsWhenThreadExitsHelper(true);
      testReturnsWhenThreadExitsHelper(false);
    }

    @Test
    public void testTimesOut() throws InterruptedException, ExecutionException, TimeoutException {
      TestThread thread = new TestThread.Builder().build();
      Future<?> future = new ThreadFuture(thread.get());

      future.get(10, TimeUnit.MILLISECONDS);

      Assert.assertFalse(future.isDone());
      Assert.assertTrue(thread.isAlive());
    }

    @Test
    public void testIsInterruptable() throws InterruptedException {
      TestThread thread = new TestThread.Builder().build();
      Future<?> future = new ThreadFuture(thread.get());
      CountDownLatch interruptLatch = new CountDownLatch(1);
      Thread interruptThread = new Thread(() -> {
        try {
          future.get();
        } catch (InterruptedException e) {
          interruptLatch.countDown();
        } catch (ExecutionException ignored) {
        }
      });
      interruptThread.start();

      interruptThread.interrupt();

      interruptLatch.await(); // This will never return unless future.get() throws an InterruptedException
      // Make sure the thread and the future aren't done yet
      Assert.assertFalse(future.isDone());
      Assert.assertTrue(thread.isAlive());
    }

    @Test
    public void testThrowsIfCancelled() throws InterruptedException {
      TestThread thread = new TestThread.Builder().build();
      Future<?> future = new ThreadFuture(thread.get());
      future.cancel(true);
      thread.join();

      Assert.assertThrows(CancellationException.class, future::get);
      Assert.assertThrows(CancellationException.class, () -> future.get(1, TimeUnit.SECONDS));
    }

    private static void testReturnsWhenThreadExitsHelper(boolean throwOnExit)
        throws InterruptedException, ExecutionException {
      TestThread thread = new TestThread.Builder().throwOnExit(throwOnExit).build();
      Future<?> future = new ThreadFuture(thread.get());
      thread.exit();

      future.get();
    }
  }

  private static class TestThread {
    private final Thread _thread;
    private final CountDownLatch _latch;
    private final boolean _throwOnExit;

    private volatile boolean _isInterrupted;

    public TestThread(boolean startOnCreation, boolean throwOnExit) {
      _thread = new Thread(this::run);
      _thread.setDaemon(true);
      _latch = new CountDownLatch(1);
      _throwOnExit = throwOnExit;

      if (startOnCreation) {
        _thread.start();
      }
    }

    public Thread get() {
      return _thread;
    }

    public void exit() throws InterruptedException {
      _latch.countDown();
      join();
    }

    public void join() throws InterruptedException {
      _thread.join();
    }

    public boolean isAlive() {
      return _thread.isAlive();
    }

    public boolean isInterrupted() {
      return _isInterrupted;
    }

    private void run() {
      try {
        _latch.await();
        if (_throwOnExit) {
          throw new RuntimeException();
        }
      } catch (InterruptedException e) {
        // We do not use Thread.currentThread().interrupt() or rely on
        // _thread.isInterrupted() because querying a thread's interrupt
        // flag after it has exited is implementation-defined (the observed
        // behavior with the JDK I was using is that the interrupted flag is
        // reset on a thread's exit).
        _isInterrupted = true;
      }
    }

    public static class Builder {
      private boolean _throwOnExit = false;
      private boolean _startOnCreation = true;

      public Builder throwOnExit(boolean throwOnExit) {
        _throwOnExit = throwOnExit;
        return this;
      }

      public Builder startOnCreation(boolean startOnCreation) {
        _startOnCreation = startOnCreation;
        return this;
      }

      public TestThread build() {
        return new TestThread(_startOnCreation, _throwOnExit);
      }
    }
  }
}
