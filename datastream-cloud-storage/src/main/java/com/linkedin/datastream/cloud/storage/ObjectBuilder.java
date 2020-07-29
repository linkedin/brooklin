/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.cloud.storage.committer.ObjectCommitter;

import com.linkedin.datastream.common.DatastreamRecordMetadata;
import com.linkedin.datastream.common.DatastreamTransientException;
import com.linkedin.datastream.common.Package;
import com.linkedin.datastream.common.VerifiableProperties;

/**
 * This is an Object Builder that writes events to the local files before submitting it for the commit to cloud storage.
 * Keeps the registry of all the active write log objects.
 */
public class ObjectBuilder extends Thread {

    private static final Logger LOG = LoggerFactory.getLogger(CloudStorageTransportProvider.class.getName());

    private final Map<String, WriteLog> _registry;
    private final BlockingQueue<Package> _packageQueue;

    private final String _ioClass;
    private final VerifiableProperties _ioProperties;
    private final String _localDir;
    private final long _maxFileSize;
    private final int _maxFileAge;
    private final int _maxInflightWriteLogCommits;
    private final ObjectCommitter _committer;

    /**
     * Constructor for ObjectBuilder.
     * @param ioClass one of the implementations of {@link com.linkedin.datastream.cloud.storage.io.File}
     * @param ioProperties configuration options for the io class
     * @param localDir local directory where the objects are created before committing to cloud storage
     * @param maxFileSize max size of the object
     * @param maxFileAge max age of the object
     * @param maxInflightWriteLogCommits max commit backlog
     * @param committer object committer
     * @param queueSize object builder's queue size
     */
    public ObjectBuilder(String ioClass,
                         VerifiableProperties ioProperties,
                         String localDir,
                         long maxFileSize,
                         int maxFileAge,
                         int maxInflightWriteLogCommits,
                         ObjectCommitter committer,
                         int queueSize) {
        this._ioClass = ioClass;
        this._ioProperties = ioProperties;
        this._localDir = localDir;
        this._maxFileSize = maxFileSize;
        this._maxFileAge = maxFileAge;
        this._maxInflightWriteLogCommits = maxInflightWriteLogCommits;
        this._committer = committer;
        this._packageQueue = new LinkedBlockingQueue<>(queueSize);
        this._registry = new HashMap<>();
    }

    /**
     * Stops the object builder thread.
     */
    public void shutdown() {
        interrupt();
    }

    /**
     * Submits a package to the queue.
     * @param aPackage package that needs to be processed
     */
    public void assign(Package aPackage) {
        try {
            _packageQueue.put(aPackage);
        } catch (InterruptedException e) {
            LOG.warn("Assign is interrupted. {}", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Takes a package of the queue and returns.
     * @return next package in the queue
     */
    private Package getNextPackage()  throws InterruptedException {
        return _packageQueue.take();
    }

    @Override
    public void run() {
        while (!isInterrupted()) {

            Package aPackage;
            try {
                aPackage = getNextPackage();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }

            try {
                if (aPackage.isDataPackage()) {
                    _registry.computeIfAbsent(aPackage.getTopic() + "-" + aPackage.getPartition(),
                            key -> new WriteLog(_ioClass,
                                    _ioProperties,
                                    _localDir,
                                    _maxFileSize,
                                    _maxFileAge,
                                    _maxInflightWriteLogCommits,
                                    _committer)).write(aPackage);
                } else {
                    // broadcast signal
                    for (Map.Entry<String, WriteLog> entry : _registry.entrySet()) {
                        entry.getValue().write(aPackage);
                    }
                }
                aPackage.markAsDelivered();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                LOG.error("Unable to write to WriteLog {}", e);
                aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                        aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), new DatastreamTransientException(e));
            } catch (IllegalStateException e) {
                LOG.error("Unable to write to WriteLog {}", e);
                aPackage.getAckCallback().onCompletion(new DatastreamRecordMetadata(
                        aPackage.getCheckpoint(), aPackage.getTopic(), aPackage.getPartition()), e);
            }
        }
        LOG.info("Object builder stopped.");
    }
}
