package com.linkedin.datastream.common;

public class AvroEncodingException extends Exception {
  private static final long serialVersionUID = 1L;

  public AvroEncodingException(Throwable t) {
    super(t);
  }

  public AvroEncodingException(String msg) {
    super(msg);
  }

  public AvroEncodingException(String message, Throwable cause) {
    super(message, cause);
  }
}

