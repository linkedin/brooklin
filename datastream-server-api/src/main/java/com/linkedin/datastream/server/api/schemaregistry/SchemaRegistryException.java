package com.linkedin.datastream.server.api.schemaregistry;

/**
 * Exception class for all the schema registry provider exceptions.
 */
public class SchemaRegistryException extends Exception {

  public SchemaRegistryException() {
    super();
  }

  public SchemaRegistryException(String message, Throwable cause) {
    super(message, cause);
  }

  public SchemaRegistryException(String message) {
    super(message);
  }

  public SchemaRegistryException(Throwable cause) {
    super(cause);
  }
}
