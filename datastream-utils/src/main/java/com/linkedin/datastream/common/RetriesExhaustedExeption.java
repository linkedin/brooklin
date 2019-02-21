package com.linkedin.datastream.common;

public class RetriesExhaustedExeption extends RuntimeException {
  private static final long serialVersionUID = 1;

  public RetriesExhaustedExeption() {
    super();
  }

  public RetriesExhaustedExeption(String message, Throwable cause) {
    super(message, cause);
  }

  public RetriesExhaustedExeption(String message) {
    super(message);
  }

  public RetriesExhaustedExeption(Throwable cause) {
    super(cause);
  }
}
