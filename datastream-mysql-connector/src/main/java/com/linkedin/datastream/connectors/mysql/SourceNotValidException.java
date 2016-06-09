package com.linkedin.datastream.connectors.mysql;

import com.linkedin.datastream.common.DatastreamException;

public class SourceNotValidException extends DatastreamException {
  private static final long serialVersionUID = 1L;

  public SourceNotValidException() {
    super();
  }

  public SourceNotValidException(String message, Throwable cause) {
    super(message, cause);
  }

  public SourceNotValidException(String message) {
    super(message);
  }

  public SourceNotValidException(Throwable cause) {
    super(cause);
  }
}
