/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.serde;

/**
 * DeSerializer interface for brooklin which is used to convert bytes to object.
 */
public interface Deserializer {

  /**
   * Deserializes given serialized object from an array of bytes to its original form.
   * @param data Array of bytes representing serialized object.
   * @return Original deserialized object.
   */
  Object deserialize(byte[] data);
}
