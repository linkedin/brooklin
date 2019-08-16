/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka.factory;

import java.util.Properties;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link com.linkedin.datastream.kafka.factory.SimpleKafkaProducerFactory}
 */
public class TestSimpleKafkaProducerFactory {
    private static final String KEY_SERIALIZER = ByteArraySerializer.class.getCanonicalName();
    private static final String VAL_SERIALIZER = ByteArraySerializer.class.getCanonicalName();

    /**
     * Assert if we are adding default properties
     */
    @Test
    public void testDefaultProducerConfig() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "MyBroker:10251");
        properties.put("security.protocol", "SSL");
        Properties actual = SimpleKafkaProducerFactory.addProducerDefaultProperties(properties);

        Properties expected = new Properties();
        expected.putAll(properties);
        expected.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                KEY_SERIALIZER);
        expected.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                VAL_SERIALIZER);

        Assert.assertEquals(actual, expected);
    }

    /**
     * Assert if we are adding override for default properties
     */
    @Test
    public void testOverrideDefaultProducerConfigs() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "MyBroker:10251");
        properties.put("security.protocol", "SSL");

        Properties additionalProperties = new Properties();
        additionalProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "my.custom.serializer");
        additionalProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "my.custom.serializer");
        additionalProperties.putAll(properties);
        Properties actual = SimpleKafkaProducerFactory.addProducerDefaultProperties(additionalProperties);

        Properties expected = new Properties();
        expected.putAll(properties);
        expected.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "my.custom.serializer");
        expected.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "my.custom.serializer");

        Assert.assertEquals(actual, expected);
    }

}
