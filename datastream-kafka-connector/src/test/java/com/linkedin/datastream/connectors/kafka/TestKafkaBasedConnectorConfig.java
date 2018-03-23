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
    Properties configProps = config.getConsumerProps();

    // copy the props over to a new object, similar to the way the connector would do it
    Properties copiedProps = new Properties();
    copiedProps.putAll(configProps);

    // verify the config entries exist on the new object
    Assert.assertNotNull(copiedProps);
    Assert.assertEquals(copiedProps.getProperty("someProperty"), "someValue");
    Assert.assertEquals(copiedProps.getProperty("someProperty1"), "someValue1");
    Assert.assertEquals(copiedProps.getProperty("someProperty2"), "someValue2");

    // verify that config entries are propagated to connectorTask
    Properties taskProps = KafkaConnectorTask.getKafkaConsumerProperties(configProps, "groupId",
        KafkaConnectionString.valueOf("kafkassl://somewhere:777/topic"));
    Assert.assertEquals(taskProps.getProperty("someProperty"), "someValue");
    Assert.assertEquals(taskProps.getProperty("someProperty1"), "someValue1");
    Assert.assertEquals(taskProps.getProperty("someProperty2"), "someValue2");
  }

}
