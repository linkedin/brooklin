/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Represents errors where one or more operations on a datastream cannot be performed because it already exists
 */
public class DatastreamAlreadyExistsException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  /**
   * Constructor
   */
  public DatastreamAlreadyExistsException() {
    super();
  }

  /**
   * Constructor
   * @param message Exception message
   * @param cause Exception cause
   */
  public DatastreamAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor
   * @param message Exception message
   */
  public DatastreamAlreadyExistsException(String message) {
    super(message);
  }

  /**
   * Constructor
   * @param cause Exception cause
   */
  public DatastreamAlreadyExistsException(Throwable cause) {
    super(cause);
  }
}
