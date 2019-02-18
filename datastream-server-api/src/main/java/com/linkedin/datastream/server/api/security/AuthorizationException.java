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

  public AuthorizationException() {
    super();
  }

  public AuthorizationException(String message, Throwable cause) {
    super(message, cause);
  }

  public AuthorizationException(String message) {
    super(message);
  }

  public AuthorizationException(Throwable cause) {
    super(cause);
  }
}
