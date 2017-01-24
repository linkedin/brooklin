package com.linkedin.datastream.kafka;

import java.net.URI;
import java.util.Objects;

import org.apache.commons.lang3.Validate;


/**
 * Helper class to work with Kafka destination URIs with ssl/plain-text support.
 */
public class KafkaDestination {
  public static final String SCHEME_KAFKA = "kafka";
  public static final String SCHEME_SECURE_KAFKA = "kafkassl";
  public static final String DESTINATION_URI_FORMAT = SCHEME_KAFKA + "://%s?%s";
  public static final String DESTINATION_URI_SSL_FORMAT = SCHEME_SECURE_KAFKA + "://%s?%s";

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
    URI u = URI.create(uri);
    String scheme = u.getScheme();
    String authority = u.getAuthority();
    String topicName = u.getQuery();
    long portNo = u.getPort();
    Validate.notBlank(authority, "Missing authority in URI: " + uri);
    Validate.notBlank(topicName, "Missing topic name in URI: " + uri);
    Validate.isTrue(portNo != -1, "Missing port number in URI: " + uri);
    boolean isSecure = scheme.equals(SCHEME_SECURE_KAFKA);
    return new KafkaDestination(u.getAuthority() + u.getPath(), topicName, isSecure);
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
