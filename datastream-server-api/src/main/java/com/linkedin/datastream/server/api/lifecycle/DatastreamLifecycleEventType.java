/**
 *  Copyright 2024 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.lifecycle;

/**
 * The set of datastream lifecycle transitions surfaced to a {@link DatastreamLifecycleListener}. Each value
 * corresponds to a mutating operation exposed by the Datastream Management Service (DMS) REST layer.
 */
public enum DatastreamLifecycleEventType {
  /** A new datastream was created. */
  CREATE,
  /** An existing datastream definition was updated. */
  UPDATE,
  /** A datastream was deleted (status moved to {@code DELETING}). */
  DELETE,
  /** A datastream was paused (status moved to {@code PAUSED}). */
  PAUSE,
  /** A datastream was resumed (status moved back to {@code READY}). */
  RESUME,
  /** A datastream was stopped (status moved to {@code STOPPED}). */
  STOP
}
