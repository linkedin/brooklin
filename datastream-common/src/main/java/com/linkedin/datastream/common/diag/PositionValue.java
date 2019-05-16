/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.io.Serializable;


/**
 * A position value describes information about a Connector consumer's position and status when reading from a source.
 *
 * Classes which implement this interface are expected to be serializable into JSON by
 * {@link com.linkedin.datastream.common.JsonUtils#toJson(Object)} and deserializable from JSON by
 * {@link com.linkedin.datastream.common.JsonUtils#fromJson(String, Class)}.
 */
public interface PositionValue extends Serializable {
}