/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Represents errors where the maximum number of retries of a particular operation have been exhausted without success
 */
public class RetriesExhaustedException extends RuntimeException {
  private static final long serialVersionUID = 1;

  /**
   * Constructor
   */
  public RetriesExhaustedException() {
    super();
  }

  /**
   * Constructor
   * @param message Exception message
   * @param cause Exception cause
   */
  public RetriesExhaustedException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor
   * @param message Exception message
   */
  public RetriesExhaustedException(String message) {
    super(message);
  }

  /**
   * Constructor
   * @param cause Exception cause
   */
  public RetriesExhaustedException(Throwable cause) {
    super(cause);
  }
}
