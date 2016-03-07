package com.linkedin.datastream.common;

/**
 * Common Datastream exception for all unchecked exceptions
 */
public class DatastreamRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1;

  public DatastreamRuntimeException() {
    super();
  }

  public DatastreamRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatastreamRuntimeException(String message) {
    super(message);
  }

  public DatastreamRuntimeException(Throwable cause) {
    super(cause);
  }
}
