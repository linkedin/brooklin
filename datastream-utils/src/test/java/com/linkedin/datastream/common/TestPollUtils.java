package com.linkedin.datastream.common;

import java.util.function.BooleanSupplier;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestPollUtils {
  @Test
  public void testpollSimple() {
    Assert.assertFalse(PollUtils.poll(() -> false, 10, 100));
    Assert.assertTrue(PollUtils.poll(() -> true, 10, 100));
  }

  @Test
  public void testpollValidatePollCompletionTime() {
    class MyCond implements BooleanSupplier {
      private final int _timeWaitMs;
      private final long _timeThen;

      public MyCond(int timeWaitMs) {
        _timeWaitMs = timeWaitMs;
        _timeThen = System.currentTimeMillis();
      }

      @Override
      public boolean getAsBoolean() {
        return System.currentTimeMillis() - _timeThen >= _timeWaitMs;
      }
    }

    long now1 = System.currentTimeMillis();
    MyCond mycond = new MyCond(400);
    Assert.assertTrue(PollUtils.poll(mycond, 100, 500));
    long now2 = System.currentTimeMillis();

    Assert.assertTrue(now2 - now1 >= 300);
  }

  @Test
  public void testpollWithPredicate() {
    long now1 = System.currentTimeMillis();
    boolean returnValue = PollUtils.poll((c) -> System.currentTimeMillis() >= now1 + c, 100, 600, 400);

    Assert.assertTrue(returnValue);
    long now2 = System.currentTimeMillis();
    Assert.assertTrue(now2 - now1 >= 350);
  }
}
