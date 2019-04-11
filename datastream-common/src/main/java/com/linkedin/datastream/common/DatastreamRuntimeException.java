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

  /**
   * Constructor for DatastreamRuntimeException
   */
  public DatastreamRuntimeException() {
    super();
  }

  /**
   * Constructor for DatastreamRuntimeException
   * @param message Exception message
   * @param cause Exception cause
   */
  public DatastreamRuntimeException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor for DatastreamRuntimeException
   * @param message Exception message
   */
  public DatastreamRuntimeException(String message) {
    super(message);
  }

  /**
   * Constructor for DatastreamRuntimeException
   * @param cause Exception cause
   */
  public DatastreamRuntimeException(Throwable cause) {
    super(cause);
  }
}
