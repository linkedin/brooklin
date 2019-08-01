/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.Arrays;
import java.util.Collections;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Tests for {@link KafkaConnectionString}
 */
public class TestKafkaConnectionString {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNull() {
    KafkaConnectionString.valueOf(null, true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmpty() {
    KafkaConnectionString.valueOf("  \t  \r\n  \r", true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoPrefix() {
    KafkaConnectionString.valueOf("hostname:666/topic", true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseWrongPrefix() {
    KafkaConnectionString.valueOf("notKafka://hostname:666/topic", true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoHost() {
    KafkaConnectionString.valueOf("kafka://:666/topic", true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoPort() {
    KafkaConnectionString.valueOf("kafka://acme.com/topic", true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmptyTopic() {
    KafkaConnectionString.valueOf("kafka://acme.com/  ", true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoTopic() {
    KafkaConnectionString.valueOf("kafka://acme.com", true);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidHostName() {
    KafkaConnectionString.valueOf("kafka://abc-def.invalid-tld:666", true);
  }

  @Test
  public void testSimpleString() {
    Assert.assertEquals(
        new KafkaConnectionString(Collections.singletonList(new KafkaBrokerAddress("somewhere", 666)),
            "topic", false),
        KafkaConnectionString.valueOf("kafka://somewhere:666/topic", true));
  }

  @Test
  public void testSimpleSslString() {
    Assert.assertEquals(
        new KafkaConnectionString(Collections.singletonList(new KafkaBrokerAddress("somewhere", 666)),
            "topic", true),
        KafkaConnectionString.valueOf("kafkassl://somewhere:666/topic", true));
  }

  @Test
  public void testInvalidHostNameWithStrictHostCheckDisabled() {
    Assert.assertEquals(
        new KafkaConnectionString(Collections.singletonList(new KafkaBrokerAddress("abc-def.invalid-tld", 666)),
            "topic", false),
        KafkaConnectionString.valueOf("kafka://abc-def.invalid-tld:666/topic", false));
}

  @Test
  public void testMultipleBrokers() {
    Assert.assertEquals(
        new KafkaConnectionString(
            Arrays.asList(
              new KafkaBrokerAddress("somewhere", 666),
              new KafkaBrokerAddress("somewhereElse", 667)
            ),
            "topic",
            false
        ),
        KafkaConnectionString.valueOf("kafka://somewhere:666,somewhereElse:667/topic", true)
    );
  }

  @Test
  public void testBrokerListSorting() {
    KafkaConnectionString connectionString = KafkaConnectionString.valueOf("kafka://a:667,b:665,a:666/topic", true);
    Assert.assertEquals(connectionString.toString(), "kafka://a:666,a:667,b:665/topic");
  }

  @Test
  public void testSslBrokerListSorting() {
    KafkaConnectionString connectionString = KafkaConnectionString.valueOf("kafkassl://a:667,b:665,a:666/topic", true);
    Assert.assertEquals(connectionString.toString(), "kafkassl://a:666,a:667,b:665/topic");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMultipleBrokersNoPort() {
    KafkaConnectionString.valueOf("kafka://somewhere:666,somewhereElse/topic", true);
  }
}
