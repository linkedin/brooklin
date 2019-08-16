/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Tests for {@link KafkaConsumerFactoryImpl}
 */
public class TestKafkaConsumerFactoryImpl {
    private static final String KEY_DESERIALIZER = ByteArrayDeserializer.class.getCanonicalName();
    private static final String VAL_DESERIALIZER = ByteArrayDeserializer.class.getCanonicalName();

    /**
     * Assert if we are adding default properties
     */
    @Test
    public void testDefaultConsumerConfig() {
        Properties properties = new Properties();
        properties.put("auto.offset.reset", "none");
        properties.put("bootstrap.servers", "MyBroker:10251");
        properties.put("enable.auto.commit", "false");
        properties.put("group.id", "groupId");
        properties.put("security.protocol", "SSL");
        Properties actual = KafkaConsumerFactoryImpl.addConsumerDefaultProperties(properties);

        Properties expected = new Properties();
        expected.putAll(properties);
        expected.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                KEY_DESERIALIZER);
        expected.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                VAL_DESERIALIZER);

        Assert.assertEquals(actual, expected);
    }

    /**
     * Assert if we are adding override for default properties
     */
    @Test
    public void testOverrideDefaultConsumerConfigs() {
        Properties properties = new Properties();
        properties.put("auto.offset.reset", "none");
        properties.put("bootstrap.servers", "MyBroker:10251");
        properties.put("enable.auto.commit", "false");
        properties.put("group.id", "groupId");
        properties.put("security.protocol", "SSL");

        Properties additionalProperties = new Properties();
        additionalProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "my.custom.deserializer");
        additionalProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "my.custom.deserializer");
        additionalProperties.putAll(properties);
        Properties actual = KafkaConsumerFactoryImpl.addConsumerDefaultProperties(additionalProperties);

        Properties expected = new Properties();
        expected.putAll(properties);
        expected.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "my.custom.deserializer");
        expected.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "my.custom.deserializer");

        Assert.assertEquals(actual, expected);
    }
}
