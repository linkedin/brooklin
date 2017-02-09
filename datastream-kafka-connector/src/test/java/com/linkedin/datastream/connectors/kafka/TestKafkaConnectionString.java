package com.linkedin.datastream.connectors.kafka;

import java.util.Arrays;
import java.util.Collections;
import org.testng.Assert;
import org.testng.annotations.Test;


public class TestKafkaConnectionString {

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNull() {
    KafkaConnectionString.valueOf(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmpty() {
    KafkaConnectionString.valueOf("  \t  \r\n  \r");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoPrefix() {
    KafkaConnectionString.valueOf("hostname:666/topic");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseWringPrefix() {
    KafkaConnectionString.valueOf("notKafka://hostname:666/topic");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoHost() {
    KafkaConnectionString.valueOf("kafka://:666/topic");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoPort() {
    KafkaConnectionString.valueOf("kafka://acme.com/topic");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseEmptyTopic() {
    KafkaConnectionString.valueOf("kafka://acme.com/  ");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testParseNoTopic() {
    KafkaConnectionString.valueOf("kafka://acme.com");
  }

  @Test
  public void testSimpleString() {
    Assert.assertEquals(
        new KafkaConnectionString(Collections.singletonList(new KafkaBrokerAddress("somewhere", 666)), "topic"),
        KafkaConnectionString.valueOf("kafka://somewhere:666/topic")
    );
  }

  @Test
  public void testMultipleBrokers() {
    Assert.assertEquals(
        new KafkaConnectionString(
            Arrays.asList(
              new KafkaBrokerAddress("somewhere", 666),
              new KafkaBrokerAddress("somewhereElse", 667)
            ),
            "topic"
        ),
        KafkaConnectionString.valueOf("kafka://somewhere:666,somewhereElse:667/topic")
    );
  }

  @Test
  public void testBrokerListSorting() {
    KafkaConnectionString connectionString = KafkaConnectionString.valueOf("kafka://a:667,b:665,a:666/topic");
    Assert.assertEquals("kafka://a:666,a:667,b:665/topic", connectionString.toString());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testMultipleBrokersNoPort() {
    KafkaConnectionString.valueOf("kafka://somewhere:666,somewhereElse/topic");
  }
}
