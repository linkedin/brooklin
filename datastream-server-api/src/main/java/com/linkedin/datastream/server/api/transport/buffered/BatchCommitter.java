/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport.buffered;

import java.util.List;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;

/**
 * Implement this interface to commit buffered writes to the destination.
 * @param <T> collection of records that represents a batch
 */
public interface BatchCommitter<T> {
    /**
     * Commits the file to cloud storage
     * @param destination destination bucket
     * @param ackCallbacks list of call backs associated to each record in the file that should be called in order
     * @param recordMetadata metadata associated to each record in the file
     * @param sourceTimestamps source timestamps of the records in the batch
     * @param callback callback that needs to be called to notify the Object Builder about the completion of the commit
     */
    void commit(T batch,
                String destination,
                List<SendCallback> ackCallbacks,
                List<DatastreamRecordMetadata> recordMetadata,
                List<Long> sourceTimestamps,
                CommitCallback callback);

    /**
     * Shutdown the committer
     */
    void shutdown();
}
