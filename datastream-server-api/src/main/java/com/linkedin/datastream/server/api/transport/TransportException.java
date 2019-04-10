/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport;

/**
 * Exception class for all the transport-related exceptions.
 */
public class TransportException extends Exception {
  private static final long serialVersionUID = 1;

  /**
   * Constructor for TransportException
   */
  public TransportException() {
    super();
  }

  /**
   * Constructor for TransportException which takes a custom message and Throwable as input.
   */
  public TransportException(String message, Throwable cause) {
    super(message, cause);
  }

  /**
   * Constructor for TransportException which takes a custom message as input.
   */
  public TransportException(String message) {
    super(message);
  }

  /**
   * Constructor for TransportException which takes a Throwable as input.
   */
  public TransportException(Throwable cause) {
    super(cause);
  }
}
