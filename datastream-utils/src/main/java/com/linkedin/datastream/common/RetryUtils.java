/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utilities for retrying function invocations
 */
public class RetryUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class.getName());

  /**
   * Keep invoking a function until it succeeds or a timeout period elapses
   * @param func function to invoke
   * @param period how long to wait between invocations if {@code func} fails
   * @param timeout how long to keep trying
   * @throws RetriesExhaustedException if {@code func} never succeeds within the specified {@code timeout}
   */
  public static <T> T retry(Supplier<T> func, Duration period, Duration timeout) {
    ExceptionTrackingMethodCaller<T> supplier = new ExceptionTrackingMethodCaller<>(func);
    boolean result = PollUtils.poll(supplier, period.toMillis(), timeout.toMillis());

    if (!result) {
      LOG.error("Retries exhausted.", supplier.getLastException());
      throw new RetriesExhaustedException(supplier.getLastException());
    }

    return supplier.getValue();
  }

  private static class ExceptionTrackingMethodCaller<U> implements BooleanSupplier {
    private static final Logger LOG = LoggerFactory.getLogger(ExceptionTrackingMethodCaller.class.getName());

    private final Supplier<U> _func;
    private U _value;
    private Exception _lastException;

    public ExceptionTrackingMethodCaller(Supplier<U> func) {
      _func = func;
    }

    public Exception getLastException() {
      return _lastException;
    }

    U getValue() {
      return _value;
    }

    @Override
    public boolean getAsBoolean() {
      try {
        _value = _func.get();
      } catch (Exception e) {
        if (e instanceof RetriableException) {
          _lastException = e;
          LOG.info("Method threw a retriable exception.", e);
          return false;
        }
        throw e;
      }

      return true;
    }
  }
}

