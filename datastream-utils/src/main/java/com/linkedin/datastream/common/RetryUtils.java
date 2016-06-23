package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;


public class RetryUtils {

  public static <T> T retry(Supplier<T> func, Duration period, Duration timeout) {
    final List<T> returnValue = new ArrayList<>();

    boolean result = PollUtils.poll(() -> {
      try {
        returnValue.add(func.get());
      } catch (Exception e) {
        if (e instanceof RetriableException) {
          return false;
        }
        throw e;
      }

      return true;
    }, period.toMillis(), timeout.toMillis());

    if (!result) {
      throw new RetriesExhaustedExeption();
    }

    return returnValue.get(0);
  }
}
