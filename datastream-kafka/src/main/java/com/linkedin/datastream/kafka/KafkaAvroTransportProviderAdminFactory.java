/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.util.Properties;

import org.apache.avro.generic.GenericRecord;

import com.linkedin.datastream.server.api.transport.TransportProviderAdmin;
import com.linkedin.datastream.server.api.transport.TransportProviderAdminFactory;

/**
 * Simple Kafka Transport provider factory that creates one kafka avro producer for the entire system
 */
public class KafkaAvroTransportProviderAdminFactory implements TransportProviderAdminFactory {

    @Override
    public TransportProviderAdmin createTransportProviderAdmin(String transportProviderName,
                                                               Properties transportProviderProperties) {
        return new KafkaTransportProviderAdmin<GenericRecord, GenericRecord>(transportProviderName, transportProviderProperties);
    }
}
