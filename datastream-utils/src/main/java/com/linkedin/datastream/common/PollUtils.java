package com.linkedin.datastream.common;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * Utility class for polling an arbitrary condition (boolean/predicate)
 * with timeout/period support.
 */
public final class PollUtils {
  /**
   * Blocking poll the condition until it's met or time exceeds timeoutMs
   * @param cond boolean supplier representing the condition
   * @param periodMs periodMs between two polling iterations (ms)
   * @param timeoutMs time before exit polling if condition is never met (ms)
   * @return true if condition is met, false otherwise
   */
  public static boolean poll(BooleanSupplier cond, long periodMs, long timeoutMs) {
    return poll((n) -> cond.getAsBoolean(), periodMs, timeoutMs, null);
  }

  /**
   * Blocking poll the condition until it's met or time exceeds timeoutMs with argument
   * @param cond predicate object representing the condition
   * @param periodMs periodMs between two polling iterations (ms)
   * @param timeoutMs time before exit polling if condition is never met (ms)
   * @param arg input argument for the predicate
   * @return true if condition is met, false otherwise
   */
  public static <T> boolean poll(Predicate<T> cond, long periodMs, long timeoutMs, T arg) {
    long elapsedMs = 0;
    if (timeoutMs > 0 && periodMs > timeoutMs) {
      return false;
    }
    while (true) {
      if (cond.test(arg)) {
        return true;
      }
      try {
        Thread.sleep(periodMs);
      } catch (InterruptedException e) {
        break;
      }
      elapsedMs += periodMs;
      if (timeoutMs > 0 && elapsedMs >= timeoutMs) {
        break;
      }
    }
    return false;
  }

  /**
   * Blocking poll the condition until it's met or time exceeds timeoutMs with supplier
   * @param cond predicate object representing the condition
   * @param periodMs periodMs between two polling iterations (ms)
   * @param timeoutMs time before exit polling if condition is never met (ms)
   * @param supplier input argument for the predicate
   * @return an Optional of the result satisfying the condition, empty if it couldn't
   */
  public static <T> Optional<T> poll(Supplier<T> supplier, Predicate<T> cond, long periodMs, long timeoutMs) {
    long elapsedMs = 0;
    if (periodMs > timeoutMs) {
      return Optional.empty();
    }
    while (true) {
      T ret = supplier.get();
      if (cond.test(ret)) {
        return Optional.of(ret);
      }
      try {
        Thread.sleep(periodMs);
      } catch (InterruptedException e) {
        break;
      }
      elapsedMs += periodMs;
      if (elapsedMs >= timeoutMs) {
        break;
      }
    }
    return Optional.empty();
  }
}
