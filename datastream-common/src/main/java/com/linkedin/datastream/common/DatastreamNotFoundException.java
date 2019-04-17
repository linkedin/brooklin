/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

/**
 * Exception when a datastream is not found.
 */
public class DatastreamNotFoundException extends DatastreamRuntimeException {
  private static final long serialVersionUID = 1;

  /**
   * Constructor
   * @param datastreamName datastream name
   * @param cause Exception cause
   */
  public DatastreamNotFoundException(String datastreamName, Throwable cause) {
    super(String.format("Datastream %s is not found", datastreamName), cause);
  }
}
