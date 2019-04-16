/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link ThreadUtils}
 */
@Test
public class TestThreadUtils {
  private void runWorker(CountDownLatch latch, boolean unkillable, ExecutorService executor) {
    final boolean[] started = {false};
    executor.submit(() -> {
      do {
        try {
          started[0] = true;
          latch.await();
          break;
        } catch (InterruptedException e) {
        }
      } while (unkillable);
    });
    // Ensure thread has started
    Assert.assertTrue(PollUtils.poll(() -> started[0], 100, 30000));
  }

  @Test
  public void testCleanShutdown() {
    Logger logger = LoggerFactory.getLogger("testCleanShutdown");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch latch = new CountDownLatch(1);
    runWorker(latch, false, executor);

    Thread t = new Thread(() -> {
      try {
        Thread.sleep(200);
        latch.countDown();
      } catch (InterruptedException e) {
        Assert.fail();
      }
    });
    t.start();

    Assert.assertTrue(ThreadUtils.shutdownExecutor(executor, Duration.ofMinutes(1), logger));
  }

  @Test
  public void testUncleanShutdown() {
    Logger logger = LoggerFactory.getLogger("testUncleanShutdown");
    ExecutorService executor = Executors.newSingleThreadExecutor();
    final CountDownLatch latch = new CountDownLatch(1);
    runWorker(latch, true, executor);

    Assert.assertFalse(ThreadUtils.shutdownExecutor(executor, Duration.ofMillis(200), logger));
    latch.countDown();
    Assert.assertTrue(ThreadUtils.shutdownExecutor(executor, Duration.ofMinutes(1), logger));
  }
}
