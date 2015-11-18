package com.linkedin.datastream.server;

/**
 * CheckpointProvider is the interface for event producer to load/save
 * checkpoints on behalf of the connectors that use common checkpoint
 * framework.
 */
public interface CheckpointProvider {
  /**
   * Load the previously saved checkpoint for the {@task}
   * @param taskName name of the task owning the checkpoint
   * @return checkpoint string if exists, null otherwise
   */
  String load(String taskName);

  /**
   * Save the checkpiont against the {@task}
   * @param taskName name of the task owning the checkpoint
   * @param checkpoint checkpoint string
   */
  void save(String taskName, String checkpoint);
}
