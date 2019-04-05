/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.security;

import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * Special exception class representing authorization errors.
 */
public class AuthorizationException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  /**
   * default constructor
   */
  public AuthorizationException() {
    super();
  }

  /**
   * constructor using message and cause
   */
  public AuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * constructor using message only
   */
  public AuthorizationException(String message) {
    super(message);
  }

  /**
   * constructor using cause only
   */
  public AuthorizationException(Throwable cause) {
    super(cause);
  }
}
