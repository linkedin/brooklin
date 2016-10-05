package com.linkedin.datastream.common;

import org.testng.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import com.codahale.metrics.Meter;

import com.linkedin.datastream.metrics.StaticBrooklinMetric;


public class TestThreadTerminationMonitor {

  private Meter meter;

  @BeforeTest
  public void prepare() {
    meter =
        (Meter) ((StaticBrooklinMetric) ThreadTerminationMonitor.getMetrics().stream().findFirst().get()).getMetric();
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
