package com.linkedin.datastream.common;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;


public class TestThreadTerminationMonitor {

  private Meter meter;

  @BeforeTest
  public void prepare() {
    meter = (Meter) ThreadTerminationMonitor.getMetrics().values().iterator().next();
    Assert.assertEquals(0, meter.getCount());
  }

  @Test
  public void testHappyDay()
      throws Exception {
    Thread thread = new Thread(() -> {
    });
    thread.join();
    Assert.assertEquals(0, meter.getCount());
  }

  @Test
  public void testLeakage()
      throws Exception {
    Thread thread = new Thread(() -> {
      throw new RuntimeException();
    });
    thread.start();
    thread.join();
    Assert.assertEquals(1, meter.getCount());
  }
}
