/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Exception related to Avro encoding issues
 */
public class AvroEncodingException extends Exception {
  private static final long serialVersionUID = 1L;

  /**
   * Constructor for AvroEncodingException
   * @param cause Exception cause
   */
  public AvroEncodingException(Throwable cause) {
    super(cause);
  }

  /**
   * Constructor for AvroEncodingException
   * @param message Exception message
   */
  public AvroEncodingException(String message) {
    super(message);
  }

  /**
   * Constructor for AvroEncodingException
   * @param message Exception message
   * @param cause Exception cause
   */
  public AvroEncodingException(String message, Throwable cause) {
    super(message, cause);
  }
}

