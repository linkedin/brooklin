package com.linkedin.datastream.common;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.function.BooleanSupplier;


public class TestPollUtils {
  @Test
  public void testpollSimple() {
    Assert.assertFalse(PollUtils.poll(() -> false, 10, 100));
    Assert.assertTrue(PollUtils.poll(() -> true, 10, 100));
  }

  @Test
  public void testpollReal() {
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
    MyCond mycond = new MyCond(500);
    PollUtils.poll(mycond, 100, 500);
    long now2 = System.currentTimeMillis();
    Assert.assertTrue(now2 - now1 >= 350 && now2 - now1 <= 650);
  }

  @Test
  public void testpollWithPredicate() {
    Integer counter = 0;
    long now1 = System.currentTimeMillis();
    PollUtils.poll((c) -> {
      c++;
      return c == 5;
    }, 100, 500, counter);
    long now2 = System.currentTimeMillis();
    Assert.assertTrue(now2 - now1 >= 350 && now2 - now1 <= 650);
  }
}
