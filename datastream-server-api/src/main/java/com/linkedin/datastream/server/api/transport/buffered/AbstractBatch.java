/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport.buffered;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.SendCallback;

/**
 * Extend this class to implement batch object for specific datastore producer
 */
public abstract class AbstractBatch {
    protected static final Logger LOG = LoggerFactory.getLogger(AbstractBatch.class.getName());

    protected final int _maxInflightWriteLogCommits;

    protected int _inflightWriteLogCommits;
    protected Object _counterLock;

    protected String _destination;
    protected List<SendCallback> _ackCallbacks;
    protected List<DatastreamRecordMetadata> _recordMetadata;
    protected List<Long> _sourceTimestamps;

    protected AbstractBatch(int maxInflightWriteLogCommits) {
        this._maxInflightWriteLogCommits = maxInflightWriteLogCommits;
        this._destination = null;
        this._ackCallbacks = new ArrayList<>();
        this._recordMetadata = new ArrayList<>();
        this._sourceTimestamps = new ArrayList<>();
        this._counterLock = new Object();
    }

    protected void waitForRoomInCommitBacklog() throws InterruptedException {
        synchronized (_counterLock) {
            if (_inflightWriteLogCommits >= _maxInflightWriteLogCommits) {
                LOG.info("Waiting for room in commit backlog, current inflight commits {} ",
                        _inflightWriteLogCommits);
            }
            while (_inflightWriteLogCommits >= _maxInflightWriteLogCommits) {
                try {
                    _counterLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    protected void waitForCommitBacklogToClear() throws InterruptedException {
        synchronized (_counterLock) {
            if (_inflightWriteLogCommits > 0) {
                LOG.info("Waiting for the commit backlog to clear.");
            }
            while (_inflightWriteLogCommits > 0) {
                try {
                    _counterLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw e;
                }
            }
        }
    }

    protected void incrementInflightWriteLogCommits() {
        synchronized (_counterLock) {
            _inflightWriteLogCommits++;
        }
    }

    protected void decrementInflightWriteLogCommitsAndNotify() {
        synchronized (_counterLock) {
            _inflightWriteLogCommits--;
            _counterLock.notify();
        }
    }

    /**
     * Adds a record to the batch
     * @param aPackage package to be buffered for eventual commit.
     */
    public abstract void write(Package aPackage) throws InterruptedException;
}
