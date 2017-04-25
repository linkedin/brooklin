package com.linkedin.datastream.avrogenerator;

public class SchemaGenerationException extends Exception {
  private static final long serialVersionUID = 1L;

  public SchemaGenerationException() {
    super();
  }

  public SchemaGenerationException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaGenerationException(String message) {
    super(message);
  }

  public SchemaGenerationException(Throwable cause) {
    super(cause);
  }
}
