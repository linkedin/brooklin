/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Represents an exception that can be retried.
 */
public class RetriableException extends RuntimeException  {
  private static final long serialVersionUID = 1;

  public RetriableException() {
    super();
  }

  public RetriableException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetriableException(String message) {
    super(message);
  }

  public RetriableException(Throwable cause) {
    super(cause);
  }
}
