package com.linkedin.datastream.common;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class RetryUtils {

  private static final Logger LOG = LoggerFactory.getLogger(RetryUtils.class.getName());

  public static <T> T retry(Supplier<T> func, Duration period, Duration timeout) {
    ExceptionTrackingMethodCaller<T> supplier = new ExceptionTrackingMethodCaller<>(func);
    boolean result = PollUtils.poll(supplier, period.toMillis(), timeout.toMillis());

    if (!result) {
      LOG.error("Retries exhausted.", supplier.getLastException());
      throw new RetriesExhaustedExeption(supplier.getLastException());
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

