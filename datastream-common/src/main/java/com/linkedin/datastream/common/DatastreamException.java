/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Common exception class for all Datastream errors.
 */
public class DatastreamException extends Exception {
  private static final long serialVersionUID = 1L;

  public DatastreamException() {
    super();
  }

  public DatastreamException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatastreamException(String message) {
    super(message);
  }

  public DatastreamException(Throwable cause) {
    super(cause);
  }
}
