/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.providers;

/**
 * An abstraction for the connector to maintain information about the progress made
 * in processing {@link com.linkedin.datastream.server.DatastreamTask}s, e.g. checkpoints/offsets.
 * Use the implementation of this interface to support custom checkpointing by the connector.
 *
 * @param <T> checkpoint value type
 */
public interface CustomCheckpointProvider<T> {

    /**
     * Update the checkpoint to the given checkpoint.
     * This checkpoint may not be persisted to underlying checkpoint store.
     * @param checkpoint new checkpoint
     */
    void updateCheckpoint(T checkpoint);

    /**
     * Rewind the checkpoint to last know safe checkpoint.
     * The safe checkpoint must be provided by the caller.
     * @param checkpoint checkpoint to rewind to
     */
    void rewindTo(T checkpoint);

    /**
     * Persist the safe checkpoint to the underlying checkpoint store
     * @param checkpoint safe checkpoint
     */
    void commit(T checkpoint);

    /**
     * close the checkpoint provider
     */
    void close();

    /**
     * get safe checkpoint
     * @return last known safe checkpoint
     */
    T getSafeCheckpoint();

    /**
     * get last committed checkpoint
     * @return last committed checkpoint
     */
    T getCommitted();
}
