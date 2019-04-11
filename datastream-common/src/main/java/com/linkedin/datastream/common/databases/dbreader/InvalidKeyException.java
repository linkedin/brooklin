/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.databases.dbreader;

/**
 * Exception to indicate the table has no valid keys the seeder can use. This can be used by applications to
 * to skip over tables that cannot be seeded if required.
 */
public class InvalidKeyException extends Exception {

  private static final long serialVersionUID = 2132150605367432532L;

  /**
   * Construct an instance of InvalidKeyException
   */
  public InvalidKeyException() {
    super();
  }

  /**
   * Construct an instance of InvalidKeyException with message
   * @param message exception information
   */
  public InvalidKeyException(String message) {
    super(message);
  }
}