package com.linkedin.datastream.common;

/**
 * Represents an exception that can be retried.
 */
public class RetriableException extends RuntimeException {
  private static final long serialVersionUID = 1;

  public RetriableException() {
    super();
  }

  public RetriableException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetriableException(String message) {
    super(message);
  }

  public RetriableException(Throwable cause) {
    super(cause);
  }
}
