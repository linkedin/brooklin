/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.cloud.storage;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;

/**
 * Simple Cloud Storage Transport provider factory that creates one producer for the entire system
 */
public class CloudStorageTransportProviderAdminFactory implements TransportProviderAdminFactory {
    @Override
    public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
                                                               Properties transportProviderProperties) {
        return new CloudStorageTransportProviderAdmin(transportProviderName, transportProviderProperties);
    }
}
