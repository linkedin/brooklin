/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Transient datastream exception, which indicates a temporary exception for an operation that should be retried.
 */
public class DatastreamTransientException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  /**
   * Constructor for DatastreamTransientException
   */
  public DatastreamTransientException() {
    super();
  }

  /**
   * Constructor for DatastreamTransientException
   * @param message Exception message
   * @param cause Exception cause
   */
  public DatastreamTransientException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor for DatastreamTransientException
   * @param message Exception message
   */
  public DatastreamTransientException(String message) {
    super(message);
  }

  /**
   * Constructor for DatastreamTransientException
   * @param cause Exception cause
   */
  public DatastreamTransientException(Throwable cause) {
    super(cause);
  }
}
