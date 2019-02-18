/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 *
 */
public class RetriesExhaustedExeption extends RuntimeException {
  private static final long serialVersionUID = 1;

  public RetriesExhaustedExeption() {
    super();
  }

  public RetriesExhaustedExeption(String message, Throwable cause) {
    super(message, cause);
  }

  public RetriesExhaustedExeption(String message) {
    super(message);
  }

  public RetriesExhaustedExeption(Throwable cause) {
    super(cause);
  }
}
