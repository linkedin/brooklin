/**
 *  Copyright 2024 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.lifecycle;

import com.linkedin.datastream.common.Datastream;

/**
 * Optional observability extension point notified of datastream lifecycle transitions driven through the
 * Datastream Management Service (DMS) REST layer: creation, update, deletion, and pause/resume/stop status
 * changes.
 *
 * <p>Implementations are typically used to emit an audit/event stream (for example to a metrics or events
 * pipeline). The DMS invokes the listener on a best-effort basis <em>after</em> the corresponding change has
 * been persisted. Implementations therefore:
 * <ul>
 *   <li>must be non-blocking (offload any I/O to their own executor), and</li>
 *   <li>must not throw — the DMS guards against listener failures, but a listener should never assume its
 *       failure will affect, or be affected by, the outcome of the REST request.</li>
 * </ul>
 *
 * <p>The default behaviour when no listener is configured is a no-op.
 */
public interface DatastreamLifecycleListener {
  /**
   * Invoked after a datastream lifecycle transition has been persisted.
   *
   * @param eventType the type of lifecycle transition
   * @param datastream the affected datastream, reflecting its post-transition state
   */
  void onDatastreamLifecycleEvent(DatastreamLifecycleEventType eventType, Datastream datastream);
}
