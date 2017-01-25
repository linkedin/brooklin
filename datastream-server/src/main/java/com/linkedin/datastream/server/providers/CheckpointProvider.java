package com.linkedin.datastream.server.providers;

import java.util.Map;

import com.linkedin.datastream.metrics.MetricsAware;
import com.linkedin.datastream.server.DatastreamTask;


/**
 * Checkpoint provider
 */
public interface CheckpointProvider extends MetricsAware {

  /**
   * update the checkpoint. This might get called every time a send succeeds. So avoid writing to durable store
   * everytime updateCheckpoint is called.
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
   * @param datastreamTask datastream tasks whose checkpoints need to be read
   * @return Map of the checkpoints associated with the datastream task.
   */
  Map<Integer, String> getCommitted(DatastreamTask datastreamTask);
}
