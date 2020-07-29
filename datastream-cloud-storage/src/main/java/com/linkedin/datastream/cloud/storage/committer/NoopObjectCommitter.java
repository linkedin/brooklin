/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage.committer;

import java.util.List;

import com.linkedin.datastream.cloud.storage.CommitCallback;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.SendCallback;
import com.linkedin.datastream.common.VerifiableProperties;

/**
 * This is an Noop Object Committer that simply discards the files to commit, analogous to /dev/null.
 */
public class NoopObjectCommitter implements ObjectCommitter {

    /**
     * Constructor for Noop Object Committer
     * @param props configuration options
     */
    public NoopObjectCommitter(VerifiableProperties props) {
    }

    @Override
    public void commit(String filePath,
                       String fileFormat,
                       String destination,
                       String topic,
                       long partition,
                       long minOffset,
                       long maxOffset,
                       List<SendCallback> ackCallbacks,
                       List<DatastreamRecordMetadata> recordMetadata,
                       CommitCallback callback) {
        for (int i = 0; i < ackCallbacks.size(); i++) {
            ackCallbacks.get(i).onCompletion(recordMetadata.get(i), null);
        }
        callback.commited();
    }

    @Override
    public void shutdown() {
    }
}
