/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import org.apache.commons.httpclient.util.URIUtil;
import org.testng.Assert;
import org.testng.annotations.Test;


@Test
public class TestKafkaDestination {
  @Test
  public void testDestinationFormatting() {
    String zkAddress = "localhost:12913/kafka-datastream";
    String topicName = "testtopic_test";
    KafkaDestination destination = new KafkaDestination(zkAddress, topicName, false);
    Assert.assertEquals(destination.getDestinationURI(),
        "kafka://localhost:12913/kafka-datastream/testtopic_test");

    destination = new KafkaDestination(zkAddress, topicName, true);
    Assert.assertEquals(destination.getDestinationURI(),
        "kafkassl://localhost:12913/kafka-datastream/testtopic_test");

    // No path case
    zkAddress = "localhost:12913";
    destination = new KafkaDestination(zkAddress, topicName, true);
    Assert.assertEquals(destination.getDestinationURI(),
        "kafkassl://localhost:12913/testtopic_test");
  }

  @Test
  public void testDestinationParsing() {
    String zkAddress = "localhost:12913/kafka-datastream";
    String topicName = "testtopic_test";
    String uri = "kafka://localhost:12913/kafka-datastream/testtopic_test";
    KafkaDestination destination = KafkaDestination.parse(uri);
    Assert.assertEquals(destination.getZkAddress(), zkAddress);
    Assert.assertEquals(destination.getTopicName(), topicName);
    Assert.assertFalse(destination.isSecure());

    // Secure case
    uri = "kafkassl://localhost:12913/kafka-datastream/testtopic_test";
    destination = KafkaDestination.parse(uri);
    Assert.assertTrue(destination.isSecure());

    // No path case
    uri = "kafkassl://localhost:12913/testtopic_test";
    destination = KafkaDestination.parse(uri);
    Assert.assertEquals(destination.getZkAddress(), "localhost:12913");
    Assert.assertEquals(destination.getTopicName(), "testtopic_test");
  }

  @Test
  public void testEscapedUri() throws Exception {
    String uri = "kafka://localhost%3A12913%2Fkafka-datastream/testtopic_test";
    KafkaDestination destination = KafkaDestination.parse(uri);
    Assert.assertEquals(destination.getZkAddress(), "localhost:12913/kafka-datastream");
    Assert.assertEquals(destination.getTopicName(), "testtopic_test");

    uri = URIUtil.encodeWithinAuthority("kafka://localhost:12913/kafka-datastream/testtopic_test");
    destination = KafkaDestination.parse(uri);
    Assert.assertEquals(destination.getZkAddress(), "localhost:12913/kafka-datastream");
    Assert.assertEquals(destination.getTopicName(), "testtopic_test");
  }

  @Test(expectedExceptions = Exception.class)
  public void testUriMissingAuthority() {
    KafkaDestination.parse("kafka:///foobar");
  }

  @Test(expectedExceptions = Exception.class)
  public void testUriMissingPort() {
    KafkaDestination.parse("kafka://abc/foobar");
  }

  @Test(expectedExceptions = Exception.class)
  public void testUriMissingTopic() {
    KafkaDestination.parse("kafka://abc:123");
  }
}
