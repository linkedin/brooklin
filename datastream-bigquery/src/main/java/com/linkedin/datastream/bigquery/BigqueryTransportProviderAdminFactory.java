/**
 *  Copyright 2020 Wayfair LLC. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.bigquery;

import java.util.Properties;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;

/**
 * Simple Bigquery Transport provider factory that creates one producer for the entire system
 */
public class BigqueryTransportProviderAdminFactory implements TransportProviderAdminFactory {
    @Override
    public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
                                                               Properties transportProviderProperties) {
        return new BigqueryTransportProviderAdmin(transportProviderName, transportProviderProperties);
    }
}
