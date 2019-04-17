/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.testutil.event.generator;

/**
 * Exception class for reporting errors when unknown types
 * are encountered while reading or writing event data/schemas
 */
public class UnknownTypeException extends Exception {
  private static final long serialVersionUID = 1L;
}
