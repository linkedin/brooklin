package com.linkedin.datastream.connectors.kafka;

import java.util.Properties;

import org.testng.Assert;
import org.testng.annotations.Test;


public class TestKafkaBasedConnectorConfig {

  /**
   * Test that the consumer configs can properly be populated when copying over from Properties object to other
   * Properties object
   */
  @Test
  public void testConsumerConfig() {
    Properties props = new Properties();
    props.put("consumer.someProperty", "someValue");
    props.put("consumer.someProperty1", "someValue1");
    props.put("consumer.someProperty2", "someValue2");

    KafkaBasedConnectorConfig config = new KafkaBasedConnectorConfig(props);
    Properties actualProps = config.getConsumerProps();

    // copy the props over to a new object, similar to the way the connector tasks will do it
    Properties taskProps = new Properties();
    taskProps.putAll(actualProps);

    // verify the config entries exist on the new object
    Assert.assertNotNull(taskProps);
    Assert.assertEquals(taskProps.getProperty("someProperty"), "someValue");
    Assert.assertEquals(taskProps.getProperty("someProperty1"), "someValue1");
    Assert.assertEquals(taskProps.getProperty("someProperty2"), "someValue2");
  }

}
