/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.avrogenerator;

/**
 * Represents an error encountered during (Avro) schema generation
 */
public class SchemaGenerationException extends Exception {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor
   */
  public SchemaGenerationException() {
    super();
  }

  /**
   * Constructor
   * @param message Exception message
   * @param cause Exception cause
   */
  public SchemaGenerationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor
   * @param message Exception message
   */
  public SchemaGenerationException(String message) {
    super(message);
  }

  /**
   * Constructor
   * @param cause Exception cause
   */
  public SchemaGenerationException(Throwable cause) {
    super(cause);
  }
}
