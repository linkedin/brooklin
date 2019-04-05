/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

import java.util.Map;

import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * An abstraction for maintaining information about the progress made
 * in processing {@link DatastreamTask}s, e.g. checkpoints/offsets.
 */
public interface CheckpointProvider extends MetricsAware {

  /**
   * Unassign datastream task. This is called when the datastream task is being reassigned from the current instance.
   * Any cleanup of the internal checkpoint state for the datastream task is performed here.
   */
  void unassignDatastreamTask(DatastreamTask task);

  /**
   * update the checkpoint. This might get called every time a send succeeds. So avoid writing to durable store
   * every time updateCheckpoint is called.
   */
  void updateCheckpoint(DatastreamTask task, int partition, String checkpoint);

  /**
   * Write the checkpoints to the store durably
   */
  void flush();

  /**
   * Get the safe checkpoints that the task has produced to. It is possible that the checkpoint provider is not
   * writing the checkpoints every time updateCheckpoint is called in which case safe checkpoints will return checkpoints
   * from the memory.
   */
  Map<Integer, String> getSafeCheckpoints(DatastreamTask task);

  /**
   * Read the checkpoints from the checkpoint store for the task
   * @param datastreamTask DatastreamTask whose checkpoints need to be read
   * @return Map of the checkpoints associated with the datastream task.
   */
  Map<Integer, String> getCommitted(DatastreamTask datastreamTask);
}
