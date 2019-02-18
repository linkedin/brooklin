/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.kafka;

import java.net.URI;
import java.util.Objects;

import org.apache.commons.httpclient.util.URIUtil;
import org.apache.commons.lang3.Validate;

import com.linkedin.datastream.common.DatastreamRuntimeException;


/**
 * Helper class to work with Kafka destination URIs with ssl/plain-text support.
 */
public class KafkaDestination {
  public static final String SCHEME_KAFKA = "kafka";
  public static final String SCHEME_SECURE_KAFKA = "kafkassl";
  public static final String DESTINATION_URI_FORMAT = SCHEME_KAFKA + "://%s/%s";
  public static final String DESTINATION_URI_SSL_FORMAT = SCHEME_SECURE_KAFKA + "://%s/%s";

  private final String _zkAddress;
  private final String _topicName;
  private final boolean _isSecure;

  public KafkaDestination(String zkAddress, String topicName, boolean isSecure) {
    _zkAddress = zkAddress;
    _topicName = topicName;
    _isSecure = isSecure;
  }

  public static KafkaDestination parse(String uri) {
    Validate.isTrue(uri.startsWith(SCHEME_KAFKA) || uri.startsWith(SCHEME_SECURE_KAFKA),
        "Invalid scheme in URI: " + uri);

    try {
      // Decode URI in case it's escaped
      uri = URIUtil.decode(uri);
    } catch (Exception e) {
      throw new DatastreamRuntimeException("Failed to decode Kafka destination URI: " + uri, e);
    }

    URI u = URI.create(uri);
    String scheme = u.getScheme();
    String zkAddress = u.getAuthority();
    String path = u.getPath();
    String topicName;
    int lastSlash = path.lastIndexOf("/");
    if (lastSlash > 0) {
      // intermediate paths are part of ZK address
      zkAddress += path.substring(0, lastSlash);
      topicName = path.substring(lastSlash + 1);
    } else {
      topicName = path.substring(1);
    }
    long portNo = u.getPort();
    Validate.notBlank(zkAddress, "Missing zkAddress in URI: " + uri);
    Validate.notBlank(topicName, "Missing topic name in URI: " + uri);
    Validate.isTrue(portNo != -1, "Missing port number in URI: " + uri);
    boolean isSecure = scheme.equals(SCHEME_SECURE_KAFKA);
    return new KafkaDestination(zkAddress, topicName, isSecure);
  }

  public String getZkAddress() {
    return _zkAddress;
  }

  public String getTopicName() {
    return _topicName;
  }

  public boolean isSecure() {
    return _isSecure;
  }

  public String getDestinationURI() {
    return String.format(_isSecure ? DESTINATION_URI_SSL_FORMAT : DESTINATION_URI_FORMAT, _zkAddress, _topicName);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    KafkaDestination that = (KafkaDestination) o;
    return _isSecure == that._isSecure
        && Objects.equals(_zkAddress, that._zkAddress)
        && Objects.equals(_topicName, that._topicName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_zkAddress, _topicName, _isSecure);
  }

  @Override
  public String toString() {
    return getDestinationURI();
  }
}
