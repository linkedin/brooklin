/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Common Datastream exception for all unchecked exceptions
 */
public class DatastreamRuntimeException extends RuntimeException {
  private static final long serialVersionUID = 1;

  public DatastreamRuntimeException() {
    super();
  }

  public DatastreamRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatastreamRuntimeException(String message) {
    super(message);
  }

  public DatastreamRuntimeException(Throwable cause) {
    super(cause);
  }
}
