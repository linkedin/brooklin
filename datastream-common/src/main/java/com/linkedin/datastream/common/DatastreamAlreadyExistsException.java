package com.linkedin.datastream.common;

public class DatastreamAlreadyExistsException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  public DatastreamAlreadyExistsException() {
    super();
  }

  public DatastreamAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatastreamAlreadyExistsException(String message) {
    super(message);
  }

  public DatastreamAlreadyExistsException(Throwable cause) {
    super(cause);
  }
}
