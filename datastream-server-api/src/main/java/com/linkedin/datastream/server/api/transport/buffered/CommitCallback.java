/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.server.api.transport.buffered;

/**
 * callback interface to acknowledge batch commits
 */
public interface CommitCallback {
    /**
     * Callback method that needs to be called when the batch commit completes
     */
    void commited();
}
