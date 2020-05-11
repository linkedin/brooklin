/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

/**
 * provides implementation to track if the package was delivered
 */
public abstract class PackageTracking {
    private volatile boolean _isDelivered;
    private Object _deliveredFlagLock;

    protected PackageTracking() {
        this._isDelivered = false;
        this._deliveredFlagLock = new Object();
    }

    /**
     * mark the package as delivered
     */
    public void markAsDelivered() {
        synchronized (_deliveredFlagLock) {
            _isDelivered = true;
            _deliveredFlagLock.notifyAll();
        }
    }

    /**
     * makes the call thread wait until the package is marked as delivered
     */
    public void waitUntilDelivered() {
        synchronized (_deliveredFlagLock) {
            if (_isDelivered) {
                return;
            }
            while (!_isDelivered) {
                try {
                    _deliveredFlagLock.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

}
