package com.linkedin.datastream.common;

import java.util.function.BooleanSupplier;
import java.util.function.Predicate;


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
  public static boolean poll(BooleanSupplier cond, Integer periodMs, Integer timeoutMs) {
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
  public static <T> boolean poll(Predicate<T> cond, Integer periodMs, Integer timeoutMs, T arg) {
    Integer elapsedMs = 0;
    boolean ret = false;
    if (periodMs > timeoutMs)
      return false;
    while (true) {
      if (cond.test(arg)) {
        ret = true;
        break;
      }
      try {
        Thread.sleep(periodMs);
      } catch (InterruptedException e) {
        break;
      }
      elapsedMs += periodMs;
      if (elapsedMs >= timeoutMs)
        break;
    }
    return ret;
  }
}
