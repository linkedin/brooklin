/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

/**
 * Exception class for all transport-related errors
 */
public class TransportException extends Exception {
  private static final long serialVersionUID = 1;

  /**
   * Constructor
   */
  public TransportException() {
    super();
  }

  /**
   * Constructor
   * @param message Exception message
   * @param cause Exception cause
   */
  public TransportException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor
   * @param message Exception message
   */
  public TransportException(String message) {
    super(message);
  }

  /**
   * Constructor
   * @param cause Exception cause
   */
  public TransportException(Throwable cause) {
    super(cause);
  }
}
