/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.io.Serializable;
import java.time.Instant;

import org.jetbrains.annotations.Nullable;

/**
 * A position key uniquely identifies the Connector's consumer and the source being consumed.
 *
 * Classes which implement this interface are expected to be serializable into JSON by
 * {@link com.linkedin.datastream.common.JsonUtils#toJson(Object)} and deserializable from JSON by
 * {@link com.linkedin.datastream.common.JsonUtils#fromJson(String, Class)}.
 *
 * Classes which implement this interface are expected to be POJOs and used as keys in {@link java.util.Map}. Thus, they
 * should also implement {@link #equals(Object)} and {@link #hashCode()} as appropriate.
 */
public interface PositionKey extends Serializable {
  /**
   * Returns the instance of Brooklin joined to the cluster which the Connector's consumer is running on.
   *
   * @return the instance name that uniquely identifies this Brooklin instance
   */
  @Nullable
  String getBrooklinInstanceName();

  /**
   * Returns the task prefix of the DatastreamTask being consumed.
   *
   * @return the task prefix of the DatastreamTask.
   */
  @Nullable
  String getDatastreamTaskPrefix();

  /**
   * Returns a unique id for the DatastreamTask assigned to the Connector.
   *
   * @return an id that uniquely identifies a DatastreamTask
   */
  @Nullable
  String getDatastreamTaskName();

  /**
   * Returns the time at which consumption by the Connector's consumer started.
   *
   * @return the consumption start time
   */
  @Nullable
  Instant getConnectorTaskStartTime();
}