package com.linkedin.datastream.server.api.security;

import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * Special exception class indicating authorization errors.
 */
public class AuthorizationException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  public AuthorizationException() {
    super();
  }

  public AuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }

  public AuthorizationException(String message) {
    super(message);
  }

  public AuthorizationException(Throwable cause) {
    super(cause);
  }
}
