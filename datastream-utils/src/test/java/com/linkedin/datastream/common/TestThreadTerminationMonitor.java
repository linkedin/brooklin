package com.linkedin.datastream.common;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.Counter;


public class TestThreadTerminationMonitor {

  private Counter counter;

  @BeforeTest
  public void prepare() {
    counter = (Counter) ThreadTerminationMonitor.getMetrics().values().iterator().next();
    Assert.assertEquals(0, counter.getCount());
  }

  @Test
  public void testHappyDay()
      throws Exception {
    Thread thread = new Thread(() -> {
    });
    thread.join();
    Assert.assertEquals(0, counter.getCount());
  }

  @Test
  public void testLeakage()
      throws Exception {
    Thread thread = new Thread(() -> {
      throw new RuntimeException();
    });
    thread.start();
    thread.join();
    Assert.assertEquals(1, counter.getCount());
  }
}
