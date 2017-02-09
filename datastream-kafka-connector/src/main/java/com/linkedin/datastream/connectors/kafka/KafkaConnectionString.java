package com.linkedin.datastream.connectors.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;


/**
 * the object representation of a kafka connection string.
 * strings are of the form:
 * kafka://[host1:port1,host2:port2...]/topicName
 */
public class KafkaConnectionString {
  private final static String PREFIX = KafkaConnector.CONNECTOR_NAME + "://";

  private final List<KafkaBrokerAddress> _brokers;
  private final String _topicName;

  public KafkaConnectionString(List<KafkaBrokerAddress> brokers, String topicName) {
    ArrayList<KafkaBrokerAddress> brokersCopy = new ArrayList<>(brokers);
    Collections.sort(brokersCopy, KafkaBrokerAddress.BY_URL);
    this._brokers = Collections.unmodifiableList(brokersCopy);
    this._topicName = topicName.trim();
  }

  public List<KafkaBrokerAddress> getBrokers() {
    return _brokers;
  }

  public String getTopicName() {
    return _topicName;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(",");
    _brokers.forEach(kafkaBrokerAddress -> joiner.add(kafkaBrokerAddress.toString()));
    return PREFIX + joiner.toString() + "/" + _topicName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaConnectionString that = (KafkaConnectionString) o;
    //note this is order-sensitive
    return Objects.equals(_brokers, that._brokers) && Objects.equals(_topicName, that._topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_brokers, _topicName);
  }

  public static KafkaConnectionString valueOf(String connectionString) throws IllegalArgumentException {
    if (connectionString == null) {
      //noinspection ConstantConditions
      badArg(connectionString);
    }
    String str = connectionString.trim();
    if (str.isEmpty() || !str.startsWith(PREFIX)) {
      badArg(connectionString);
    }
    str = str.substring(PREFIX.length());
    int topicIndex = str.lastIndexOf("/");
    if (topicIndex < 0) {
      badArg(connectionString);
    }
    String topicName = str.substring(topicIndex + 1).trim();
    if (topicName.isEmpty()) {
      badArg(connectionString);
    }
    str = str.substring(0, topicIndex);
    String[] hosts = str.split("\\s*,\\s*");
    if (hosts.length < 1) {
      badArg(connectionString);
    }
    List<KafkaBrokerAddress> brokers = new ArrayList<>(hosts.length);
    for (String host : hosts) {
      brokers.add(KafkaBrokerAddress.valueOf(host));
    }
    return new KafkaConnectionString(brokers, topicName);
  }

  private static void badArg(String arg) throws IllegalArgumentException {
    throw new IllegalArgumentException(String.valueOf(arg) + " is not a valid kafka connection string");
  }
}
