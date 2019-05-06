/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common.diag;

import java.time.Instant;

import org.jetbrains.annotations.Nullable;

/**
 * A position key uniquely identifies the Connector's consumer and the source being consumed.
 *
 * Classes which implement this interface are expected to be serializable into JSON by
 * {@link com.linkedin.datastream.common.JsonUtils#toJson(Object)} and deserializable from JSON by
 * {@link com.linkedin.datastream.common.JsonUtils#fromJson(String, Class)}.
 *
 * Classes which implement this interface are expected to be POJOs and used as keys in {@link java.util.Map}. Thus, this
 * class implement {@link #equals(Object)} and {@link #hashCode()} as appropriate.
 */
public interface PositionKey {

  /**
   * Gets the type of position data to be described. For example, "Kafka" if we are describing the status of a Kafka
   * consumer reading from a topic partition.
   *
   * @return the type of position data to be described
   */
  @Nullable
  String getType();

  /**
   * Returns the instance of Brooklin joined to the cluster which the Connector's consumer is running on.
   *
   * @return the instance name that uniquely identifies this Brooklin instance
   */
  @Nullable
  String getBrooklinInstance();

  /**
   * Returns a unique id for the task which the Connector's consumer is running in.
   *
   * @return an id that uniquely identifies a task
   */
  @Nullable
  String getBrooklinTaskId();

  /**
   * Returns the time at which consumption by the Connector's consumer started.
   *
   * @return the consumption start time
   */
  @Nullable
  Instant getTaskStartTime();
}