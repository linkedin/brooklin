package com.linkedin.datastream.common;

/**
 * Transient datastream exception, it indicates no need to pause the datastream
 */
public class DatastreamTransientException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  public DatastreamTransientException() {
    super();
  }

  public DatastreamTransientException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatastreamTransientException(String message) {
    super(message);
  }

  public DatastreamTransientException(Throwable cause) {
    super(cause);
  }
}
