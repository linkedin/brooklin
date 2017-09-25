package com.linkedin.datastream.connectors.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;
import javax.annotation.Nullable;


/**
 * the object representation of a kafka connection string.
 * strings are of the form:
 * kafka://[host1:port1,host2:port2...]/topicName
 *
 * We also support regular expressions for topicName, using the following syntax:
 * kafka://[host1:port1,host2:port2...]/pattern:topicNameRegEx
 */
public class KafkaConnectionString {
  private final static String PREFIX = KafkaConnector.CONNECTOR_NAME + "://";
  public static final String PATTERN = "pattern:";

  private final List<KafkaBrokerAddress> _brokers;
  private final String _topicName;
  private final String _topicPattern;
  private final boolean _isPattern;

  /* package */ KafkaConnectionString(List<KafkaBrokerAddress> brokers, boolean isPattern, String nameOrPattern) {
    ArrayList<KafkaBrokerAddress> brokersCopy = new ArrayList<>(brokers);
    brokersCopy.sort(KafkaBrokerAddress.BY_URL);
    _brokers = Collections.unmodifiableList(brokersCopy);
    _isPattern = isPattern;
    if (isPattern) {
      _topicName = null;
      _topicPattern = nameOrPattern.trim();
    } else {
      _topicName = nameOrPattern.trim();
      _topicPattern = null;
    }
  }

  public List<KafkaBrokerAddress> getBrokers() {
    return _brokers;
  }

  public boolean isPattern() {
    return _isPattern;
  }

  @Nullable
  public String getTopicName() {
    return _topicName;
  }

  @Nullable
  public String getTopicPattern() {
    return _topicPattern;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(",");
    _brokers.forEach(kafkaBrokerAddress -> joiner.add(kafkaBrokerAddress.toString()));
    return PREFIX + joiner.toString() + "/" + (_isPattern ? PATTERN : "") + _topicName;
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
    return Objects.equals(_brokers, that._brokers) && Objects.equals(_topicName, that._topicName)
        && Objects.equals(_isPattern, that._isPattern);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_brokers, _topicName, _isPattern);
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
    String topicNameOrPattern = str.substring(topicIndex + 1).trim();
    if (topicNameOrPattern.isEmpty()) {
      badArg(connectionString);
    }
    boolean isPattern = topicNameOrPattern.startsWith(PATTERN);
    if (isPattern) {
      topicNameOrPattern = topicNameOrPattern.substring(PATTERN.length());
    }
    str = str.substring(0, topicIndex);
    List<KafkaBrokerAddress> brokers = parseBrokers(str);
    return new KafkaConnectionString(brokers, isPattern, topicNameOrPattern);
  }

  public static List<KafkaBrokerAddress> parseBrokers(String brokersValue) {

    String[] hosts = brokersValue.split("\\s*,\\s*");
    if (hosts.length < 1) {
      badArg(brokersValue);
    }
    List<KafkaBrokerAddress> brokers = new ArrayList<>(hosts.length);
    for (String host : hosts) {
      brokers.add(KafkaBrokerAddress.valueOf(host));
    }
    return brokers;
  }

  private static void badArg(String arg) throws IllegalArgumentException {
    throw new IllegalArgumentException(String.valueOf(arg) + " is not a valid kafka connection string");
  }
}
