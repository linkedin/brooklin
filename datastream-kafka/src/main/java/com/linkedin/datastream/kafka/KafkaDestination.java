package com.linkedin.datastream.kafka;

import java.net.URI;


public class KafkaDestination {

  private String _zkAddress;
  private String _topicName;

  public KafkaDestination(String zkAddress, String topicName) {
    _zkAddress = zkAddress;
    _topicName = topicName;
  }

  public static KafkaDestination parseKafkaDestinationUri(String kafkaDestinationUri) {
    URI kafkaUri = URI.create(kafkaDestinationUri);
    String zkAddress = kafkaUri.getAuthority();
    String topicName = kafkaUri.getPath().replace("/", "");
    return new KafkaDestination(zkAddress, topicName);
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public String topicName() {
    return _topicName;
  }
}
