/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

public class DatastreamAlreadyExistsException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  public DatastreamAlreadyExistsException() {
    super();
  }

  public DatastreamAlreadyExistsException(String message, Throwable cause) {
    super(message, cause);
  }

  public DatastreamAlreadyExistsException(String message) {
    super(message);
  }

  public DatastreamAlreadyExistsException(Throwable cause) {
    super(cause);
  }
}
