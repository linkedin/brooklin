/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport.buffered;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.linkedin.datastream.common.Package;

/**
 * Extend this class to implement batch builder for buffered producer
 */
public abstract class AbstractBatchBuilder extends Thread {

    protected static final Logger LOG = LoggerFactory.getLogger(AbstractBatchBuilder.class.getName());


    protected final Map<String, AbstractBatch> _registry;
    protected final BlockingQueue<Package> _packageQueue;

    protected AbstractBatchBuilder(int queueSize) {
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
    protected Package getNextPackage()  throws InterruptedException {
        return _packageQueue.take();
    }
}
