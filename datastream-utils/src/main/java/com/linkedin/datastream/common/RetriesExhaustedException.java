/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

public class RetriesExhaustedException extends RuntimeException {
  private static final long serialVersionUID = 1;

  public RetriesExhaustedException() {
    super();
  }

  public RetriesExhaustedException(String message, Throwable cause) {
    super(message, cause);
  }

  public RetriesExhaustedException(String message) {
    super(message);
  }

  public RetriesExhaustedException(Throwable cause) {
    super(cause);
  }
}
