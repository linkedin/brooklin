/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.Objects;
import java.util.Optional;
import java.util.function.BooleanSupplier;

/**
 * Utility class for polling an arbitrary condition (boolean/predicate)
 * with timeout/period support.
 */
public final class PollUtils {

  /**
   * Interruptable version of {@link java.util.function.Supplier} for use with Poll functions
   *
   * <p>There is no requirement that a new or distinct result be returned each
   * time the supplier is invoked.
   *
   * <p>This is a <a href="package-summary.html">functional interface</a>
   * whose functional method is {@link #get()} which can throw InterruptedException
   *
   * @param <T> the type of results supplied by this supplier
   */
  @FunctionalInterface
  public interface InterruptableSupplier<T> {
    T get() throws InterruptedException;
  }

  /**
   * Interruptable version of {@link java.util.function.Predicate} for use with Poll functions
   *
   * <p>This is a <a href="package-summary.html">functional interface</a>
   * whose functional method is {@link #test(Object)} which can throw
   * InterruptedException
   *
   * @param <T> the type of the input to the predicate
   */
  @FunctionalInterface
  public interface InterruptablePredicate<T> {

    boolean test(T t) throws InterruptedException;

    default InterruptablePredicate<T> and(InterruptablePredicate<? super T> other) {
      Objects.requireNonNull(other);
      return (t) -> test(t) && other.test(t);
    }

    default InterruptablePredicate<T> negate() {
      return (t) -> !test(t);
    }


    default InterruptablePredicate<T> or(InterruptablePredicate<? super T> other) {
      Objects.requireNonNull(other);
      return (t) -> test(t) || other.test(t);
    }

    static <T> InterruptablePredicate<T> isEqual(Object targetRef) {
      return (null == targetRef) ? Objects::isNull : object -> targetRef.equals(object);
    }
  }

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
  public static <T> boolean poll(InterruptablePredicate<T> cond, long periodMs, long timeoutMs, T arg) {
    long elapsedMs = 0;
    if (timeoutMs > 0 && periodMs > timeoutMs) {
      return false;
    }
    while (true) {
      try {
        if (cond.test(arg)) {
          return true;
        }
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
   * Blocking poll the condition until it's met or time exceeds timeoutMs with interruptableSupplier
   * @param cond predicate object representing the condition
   * @param periodMs periodMs between two polling iterations (ms)
   * @param timeoutMs time before exit polling if condition is never met (ms)
   * @param interruptableSupplier input argument for the predicate
   * @return an Optional of the result satisfying the condition, empty if it couldn't
   */
  public static <T> Optional<T> poll(InterruptableSupplier<T> interruptableSupplier, InterruptablePredicate<T> cond,
      long periodMs, long timeoutMs) {
    long elapsedMs = 0;
    if (periodMs > timeoutMs) {
      return Optional.empty();
    }
    while (true) {
      T ret;
      try {
        ret = interruptableSupplier.get();
        if (cond.test(ret)) {
          return Optional.of(ret);
        }
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
