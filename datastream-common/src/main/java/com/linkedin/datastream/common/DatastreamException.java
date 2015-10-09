package com.linkedin.datastream.common;

/**
 * Common exception class for all Datastream errors.
 */
public class DatastreamException extends Exception {
  private static final long serialVersionUID = 1L;

  public DatastreamException() {
    super();
  }

  public DatastreamException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatastreamException(String message) {
    super(message);
  }

  public DatastreamException(Throwable cause) {
    super(cause);
  }
}
