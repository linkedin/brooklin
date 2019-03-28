/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.connectors.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;


/**
 * the object representation of a Kafka connection string.
 * strings are of the form:
 * kafka://[host1:port1,host2:port2...]/topicName
 */
public class KafkaConnectionString {
  public static final String BROKER_LIST_DELIMITER = ",";
  private static final String PREFIX_SCHEME_KAFKA = "kafka://";
  private static final String PREFIX_SCHEME_SECURE_KAFKA = "kafkassl://";

  private final List<KafkaBrokerAddress> _brokers;
  private final String _topicName;
  private final boolean _isSecure;

  public KafkaConnectionString(List<KafkaBrokerAddress> brokers, String topicName, boolean isSecure) {
    ArrayList<KafkaBrokerAddress> brokersCopy = new ArrayList<>(brokers);
    Collections.sort(brokersCopy, KafkaBrokerAddress.BY_URL);
    this._brokers = Collections.unmodifiableList(brokersCopy);
    this._topicName = topicName.trim();
    _isSecure = isSecure;
  }

  public List<KafkaBrokerAddress> getBrokers() {
    return _brokers;
  }

  public String getTopicName() {
    return _topicName;
  }

  public boolean isSecure() {
    return _isSecure;
  }

  @Override
  public String toString() {
    StringJoiner joiner = new StringJoiner(",");
    _brokers.forEach(kafkaBrokerAddress -> joiner.add(kafkaBrokerAddress.toString()));
    return (_isSecure ? PREFIX_SCHEME_SECURE_KAFKA : PREFIX_SCHEME_KAFKA) + joiner.toString() + "/" + _topicName;
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
        && Objects.equals(_isSecure, that._isSecure);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_brokers, _topicName, _isSecure);
  }

  public static KafkaConnectionString valueOf(String connectionString) throws IllegalArgumentException {
    if (connectionString == null) {
      //noinspection ConstantConditions
      badArg(connectionString);
    }
    String str = connectionString.trim();
    boolean isSecure = false;
    if (str.startsWith(PREFIX_SCHEME_KAFKA)) {
      str = str.substring(PREFIX_SCHEME_KAFKA.length());
      isSecure = false;
    } else if (str.startsWith(PREFIX_SCHEME_SECURE_KAFKA)) {
      str = str.substring(PREFIX_SCHEME_SECURE_KAFKA.length());
      isSecure = true;
    } else {
      badArg(connectionString);
    }
    int topicIndex = str.lastIndexOf("/");
    if (topicIndex < 0) {
      badArg(connectionString);
    }
    String topicName = str.substring(topicIndex + 1).trim();
    if (topicName.isEmpty()) {
      badArg(connectionString);
    }
    str = str.substring(0, topicIndex);
    List<KafkaBrokerAddress> brokers = parseBrokers(str);
    return new KafkaConnectionString(brokers, topicName, isSecure);
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
