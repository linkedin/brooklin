/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link DurableScheduledService}.
 */
@Test
public class TestDurableScheduledService {

  private static final Logger LOG = LoggerFactory.getLogger(TestDurableScheduledService.class);

  private static final Duration RUN_FREQUENCY = Duration.ofMillis(10);
  private static final Duration RUN_TIMEOUT = Duration.ofMillis(100);
  private static final Duration RESTART_TIMEOUT = Duration.ofMillis(2500);

  @Test
  public void testAlwaysHealthy() throws Exception {
    final AtomicLong runsCompleted = new AtomicLong(0);
    final List<Thread> taskThreads = Collections.synchronizedList(new ArrayList<>());
    final DurableScheduledService service =
        new DurableScheduledService("testAlwaysHealthy", RUN_FREQUENCY, RUN_TIMEOUT, RUN_TIMEOUT) {
          @Override
          protected void startUp() {
            LOG.info(Thread.currentThread().getName() + " startUp");
          }

          @Override
          protected void runOneIteration() {
            LOG.info(Thread.currentThread().getName() + " runOneIteration");
            synchronized (taskThreads) {
              if (!taskThreads.contains(Thread.currentThread())) {
                taskThreads.add(Thread.currentThread());
              }
            }
            runsCompleted.incrementAndGet();
          }

          @Override
          protected void shutDown() {
            LOG.info(Thread.currentThread().getName() + " shutDown");
          }
        };
    service.startAsync().awaitRunning();

    // Perform a sample of runs
    long numRuns = 2 * ((RUN_TIMEOUT.toMillis() / RUN_FREQUENCY.toMillis()) + 1);
    Duration runDuration = RESTART_TIMEOUT.multipliedBy(numRuns);
    Instant expiration = Instant.now().plus(runDuration);
    while (runsCompleted.get() < numRuns && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }

    // Stop the service
    service.stopAsync().awaitTerminated();

    // Ensure we completed our task within the timeout
    Assert.assertTrue(numRuns <= runsCompleted.get());

    // Ensure all threads are cleaned up
    Assert.assertTrue(taskThreads.stream().noneMatch(Thread::isAlive));

    // We should only have ever seen one thread over all our runs because they all should have succeeded
    Assert.assertEquals(taskThreads.size(), 1);
  }

  @Test
  public void testAlwaysThrows() throws Exception {
    final AtomicLong exceptionalRuns = new AtomicLong(0);
    final List<Thread> taskThreads = Collections.synchronizedList(new ArrayList<>());
    final DurableScheduledService service =
        new DurableScheduledService("testAlwaysThrows", RUN_FREQUENCY, RUN_TIMEOUT, RUN_TIMEOUT) {
          @Override
          protected void startUp() {
            LOG.info(Thread.currentThread().getName() + " startUp");
          }

          @Override
          protected void runOneIteration() throws Exception {
            try {
              LOG.info(Thread.currentThread().getName() + " runOneIteration");
              synchronized (taskThreads) {
                if (!taskThreads.contains(Thread.currentThread())) {
                  taskThreads.add(Thread.currentThread());
                }
              }
              throw new Exception("Expected");
            } catch (Exception e) {
              exceptionalRuns.incrementAndGet();
              throw e;
            }
          }

          @Override
          protected void shutDown() {
            LOG.info(Thread.currentThread().getName() + " shutDown");
          }
        };
    service.startAsync().awaitRunning();

    // Perform a sample of runs
    long numRuns = 2 * ((RUN_TIMEOUT.toMillis() / RUN_FREQUENCY.toMillis()) + 1);
    Duration runDuration = RESTART_TIMEOUT.multipliedBy(numRuns);
    Instant expiration = Instant.now().plus(runDuration);
    while (exceptionalRuns.get() < numRuns && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }

    // Stop the service
    service.stopAsync().awaitTerminated();

    // Ensure we completed our task within the timeout
    Assert.assertTrue(numRuns <= exceptionalRuns.get());

    // Ensure all threads are cleaned up
    Assert.assertTrue(taskThreads.stream().noneMatch(Thread::isAlive));

    // We should have a thread for every run since we had a failure on each thread
    Assert.assertEquals(taskThreads.size(), exceptionalRuns.get());
  }

  @Test
  public void testAlwaysBlock() throws Exception {
    final AtomicLong runsStarted = new AtomicLong(0);
    final List<Thread> taskThreads = Collections.synchronizedList(new ArrayList<>());
    final DurableScheduledService service =
        new DurableScheduledService("testAlwaysBlocks", RUN_FREQUENCY, RUN_TIMEOUT, RUN_TIMEOUT) {
          @Override
          protected void startUp() {
            LOG.info(Thread.currentThread().getName() + " startUp");
          }

          @Override
          protected void runOneIteration() throws Exception {
            LOG.info(Thread.currentThread().getName() + " runOneIteration");
            runsStarted.incrementAndGet();
            synchronized (taskThreads) {
              if (!taskThreads.contains(Thread.currentThread())) {
                taskThreads.add(Thread.currentThread());
              }
            }
            while (true) {
              Thread.sleep(RUN_FREQUENCY.toMillis());
            }
          }

          @Override
          protected void shutDown() {
            LOG.info(Thread.currentThread().getName() + " shutDown");
          }
        };
    service.startAsync().awaitRunning();

    // Perform a sample of runs
    long numRuns = 2 * ((RUN_TIMEOUT.toMillis() / RUN_FREQUENCY.toMillis()) + 1);
    Duration runDuration = RESTART_TIMEOUT.multipliedBy(numRuns);
    Instant expiration = Instant.now().plus(runDuration);
    while (runsStarted.get() < numRuns && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }

    // Stop the service
    service.stopAsync().awaitTerminated();

    // Ensure we completed our task within the timeout
    Assert.assertTrue(numRuns <= runsStarted.get());

    // Ensure all threads are cleaned up
    Assert.assertTrue(taskThreads.stream().noneMatch(Thread::isAlive));

    // We should have seen a thread for every run since we had a failure on each thread
    Assert.assertEquals(taskThreads.size(), runsStarted.get());
  }

  @Test
  public void testAlwaysIgnoreInterrupts() throws Exception {
    final AtomicLong runsStarted = new AtomicLong(0);
    final List<Thread> taskThreads = Collections.synchronizedList(new ArrayList<>());
    final DurableScheduledService service =
        new DurableScheduledService("testAlwaysIgnoreInterrupts", RUN_FREQUENCY, RUN_TIMEOUT, RUN_TIMEOUT) {
          @Override
          protected void startUp() {
            LOG.info(Thread.currentThread().getName() + " startUp");
          }

          @Override
          protected void runOneIteration() {
            LOG.info(Thread.currentThread().getName() + " runOneIteration");
            runsStarted.incrementAndGet();
            synchronized (taskThreads) {
              if (!taskThreads.contains(Thread.currentThread())) {
                taskThreads.add(Thread.currentThread());
              }
            }
            while (true) {
              try {
                Thread.sleep(RUN_FREQUENCY.toMillis());
              } catch (Exception e) {
                LOG.debug("Exception caught and discarded in thread {}", Thread.currentThread().getName());
              }
            }
          }

          @Override
          protected void shutDown() {
            LOG.info(Thread.currentThread().getName() + " shutDown");
          }
        };

    // Start the service
    service.startAsync().awaitRunning();

    // Perform a sample of runs
    long numRuns = 2 * ((RUN_TIMEOUT.toMillis() / RUN_FREQUENCY.toMillis()) + 1);
    Duration runDuration = RESTART_TIMEOUT.multipliedBy(numRuns);
    Instant expiration = Instant.now().plus(runDuration);
    while (runsStarted.get() < numRuns && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }

    // Stop the service
    service.stopAsync().awaitTerminated();

    // Ensure we completed our task within the timeout
    Assert.assertTrue(numRuns <= runsStarted.get());

    // Ensure all threads are cleaned up
    Assert.assertTrue(taskThreads.stream().noneMatch(Thread::isAlive));

    // We should have seen a thread for every run since we had a failure on each thread
    Assert.assertEquals(taskThreads.size(), runsStarted.get());
  }

  @Test
  public void testExtendedRun() throws Exception {
    final AtomicLong runsStarted = new AtomicLong(0);
    final AtomicLong runsSucceeded = new AtomicLong(0);
    final List<Thread> taskThreads = Collections.synchronizedList(new ArrayList<>());
    final DurableScheduledService service =
        new DurableScheduledService("testExtendedRun", RUN_FREQUENCY, RUN_TIMEOUT, RUN_TIMEOUT) {
          final Random _random = new Random();

          @Override
          protected void startUp() {
            LOG.info(Thread.currentThread().getName() + " startUp");
          }

          @Override
          protected void runOneIteration() throws Exception {
            LOG.info(Thread.currentThread().getName() + " runOneIteration");
            runsStarted.incrementAndGet();
            synchronized (taskThreads) {
              if (!taskThreads.contains(Thread.currentThread())) {
                taskThreads.add(Thread.currentThread());
              }
            }
            int outcome = _random.nextInt(4);
            if (outcome == 0) {
              // Success
              runsSucceeded.incrementAndGet();
            } else if (outcome == 1) {
              // Throws
              throw new Exception("Expected");
            } else if (outcome == 2) {
              // Blocks
              while (true) {
                Thread.sleep(RUN_FREQUENCY.toMillis());
              }
            } else if (outcome == 3) {
              // Ignores interruption
              while (true) {
                try {
                  Thread.sleep(RUN_FREQUENCY.toMillis());
                } catch (Exception e) {
                  LOG.debug("Exception caught and discarded in thread {}", Thread.currentThread().getName());
                }
              }
            } else {
              throw new IllegalStateException("Unknown outcome");
            }
          }

          @Override
          protected void shutDown() {
            LOG.info(Thread.currentThread().getName() + " shutDown");
          }
        };

    // Start the service
    service.startAsync().awaitRunning();

    // Perform a sample of runs
    long numRuns = 2 * ((RUN_TIMEOUT.toMillis() / RUN_FREQUENCY.toMillis()) + 1);
    Duration runDuration = RESTART_TIMEOUT.multipliedBy(numRuns);
    Instant expiration = Instant.now().plus(runDuration);
    while (runsStarted.get() < numRuns && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }

    // Stop the service
    service.stopAsync().awaitTerminated();

    // Ensure we completed our task within the timeout
    Assert.assertTrue(numRuns <= runsStarted.get());

    // Ensure all threads are cleaned up
    Assert.assertTrue(taskThreads.stream().noneMatch(Thread::isAlive));

    // We should see less thread utilization than in an all-failure scenario
    Assert.assertTrue(taskThreads.size() <= runsStarted.get());
    Assert.assertTrue(taskThreads.size() + runsSucceeded.get() >= runsStarted.get());
  }

  @Test
  public void testStartWithLeak() throws Exception {
    final AtomicLong runsStarted = new AtomicLong(0);
    final DurableScheduledService service =
        new DurableScheduledService("testStartWithLeak", RUN_FREQUENCY, RUN_TIMEOUT, RUN_TIMEOUT) {
          @Override
          protected void startUp() {
            LOG.info(Thread.currentThread().getName() + " startUp");
          }

          @Override
          protected void runOneIteration() {
            LOG.info(Thread.currentThread().getName() + " runOneIteration");
            runsStarted.incrementAndGet();
          }

          @Override
          protected void shutDown() {
            LOG.info(Thread.currentThread().getName() + " shutDown");
          }

          @Override
          protected boolean hasLeaked() {
            return true;
          }
        };

    // Start the service
    service.startAsync().awaitRunning();

    // Wait for the service to detect the leak and halt
    Instant expiration = Instant.now().plus(RESTART_TIMEOUT);
    while (service.isRunning() && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }

    // Stop the service (redundant)
    service.stopAsync().awaitTerminated();

    // Assert that no runs occurred
    Assert.assertEquals(runsStarted.get(), 0);
  }

  @Test
  public void testLeak() throws Exception {
    final AtomicBoolean leaked = new AtomicBoolean(false);
    final AtomicLong runsStarted = new AtomicLong(0);
    final List<Thread> taskThreads = Collections.synchronizedList(new ArrayList<>());
    final DurableScheduledService service =
        new DurableScheduledService("testStartWithLeak", RUN_FREQUENCY, RUN_TIMEOUT, RUN_TIMEOUT) {
          @Override
          protected void startUp() {
            LOG.info(Thread.currentThread().getName() + " startUp");
          }

          @Override
          protected void runOneIteration() {
            LOG.info(Thread.currentThread().getName() + " runOneIteration");
            runsStarted.incrementAndGet();
            synchronized (taskThreads) {
              if (!taskThreads.contains(Thread.currentThread())) {
                taskThreads.add(Thread.currentThread());
              }
            }
          }

          @Override
          protected void shutDown() {
            LOG.info(Thread.currentThread().getName() + " shutDown");
          }

          @Override
          protected boolean hasLeaked() {
            return leaked.get();
          }
        };

    // Start the service
    service.startAsync().awaitRunning();

    // Wait for the service to perform at least a few runs
    Instant expiration = Instant.now().plus(RESTART_TIMEOUT);
    while (runsStarted.get() < 2 && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }
    Assert.assertTrue(runsStarted.get() >= 2);

    // Mark the service as leaked
    leaked.set(true);

    // Wait for the service to detect the leak and halt
    expiration = Instant.now().plus(RESTART_TIMEOUT);
    while (service.isRunning() && Instant.now().isBefore(expiration)) {
      Thread.sleep(RUN_FREQUENCY.toMillis());
    }

    // Wait for the service to shutdown
    service.awaitTerminated();

    // Ensure the threads are dead
    Assert.assertTrue(taskThreads.stream().noneMatch(Thread::isAlive));
  }
}