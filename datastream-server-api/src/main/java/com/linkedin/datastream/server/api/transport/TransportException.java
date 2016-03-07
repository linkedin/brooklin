package com.linkedin.datastream.server.api.transport;

/**
 * Exception class for all the transport related exceptions.
 */
public class TransportException extends Exception {
  private static final long serialVersionUID = 1;

  public TransportException() {
    super();
  }

  public TransportException(String message, Throwable cause) {
    super(message, cause);
  }

  public TransportException(String message) {
    super(message);
  }

  public TransportException(Throwable cause) {
    super(cause);
  }
}
