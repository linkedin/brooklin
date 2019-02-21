/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
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
