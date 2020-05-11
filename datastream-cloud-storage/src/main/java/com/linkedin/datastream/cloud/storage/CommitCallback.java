/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

/**
 * Callback interface that the object builder needs to implement to get notified when the commit completes.
 */
 public interface CommitCallback {
    /**
     * Callback method that needs to be called when the object commit completes
     */
    void commited();
}
