/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.connector;

import com.linkedin.datastream.common.DatastreamException;

/**
 * Represents datastream config validation errors
 */
public class DatastreamValidationException extends DatastreamException {
  private static final long serialVersionUID = 1;

  /**
   * Constructor
   */
  public DatastreamValidationException() {
    super();
  }

  /**
   * Constructor
   * @param message Exception message
   * @param cause Exception cause
   */
  public DatastreamValidationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor
   * @param message Exception message
   */
  public DatastreamValidationException(String message) {
    super(message);
  }

  /**
   * Constructor
   * @param cause Exception cause
   */
  public DatastreamValidationException(Throwable cause) {
    super(cause);
  }
}
