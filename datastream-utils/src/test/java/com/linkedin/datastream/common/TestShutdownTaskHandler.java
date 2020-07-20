/**
 *  Copyright 2020 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests for {@link ShutdownTaskHandler}
 */
public class TestShutdownTaskHandler {
  private static final long MAX_SLEEP = 1000000000L;
  private ShutdownTaskHandler _shutdownTaskHandler;
  @BeforeMethod
  public void setup() {
    _shutdownTaskHandler = new ShutdownTaskHandler();
    _shutdownTaskHandler.start();
  }

  @AfterMethod
  public void teardown() {
    _shutdownTaskHandler.stop();
  }

  @Test
  public void testCancelFutureTask() {
    _shutdownTaskHandler.start();
    AtomicBoolean taskComplete = new AtomicBoolean(false);
    Future<?> x = _shutdownTaskHandler.submit(() -> {
      Thread.sleep(MAX_SLEEP);
      taskComplete.set(true);
      return 0;
    });
    _shutdownTaskHandler.verifyStopOrCancelTaskFuture(x);
    Assert.assertTrue(x.isDone() && !taskComplete.get());
  }

  @Test
  public void testCancelTaskWhenShutdownTaskHandlerIsStopped() {
    AtomicBoolean taskComplete = new AtomicBoolean(false);
    Future<?> x = _shutdownTaskHandler.submit(() -> {
      Thread.sleep(MAX_SLEEP);
      taskComplete.set(true);
      return 0;
    });
    _shutdownTaskHandler.stop();
    Assert.assertTrue(x.isDone() && !taskComplete.get());
  }
}
