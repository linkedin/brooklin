/**
 *  Copyright 2019 LinkedIn Corporation. All rights reserved.
 *  Licensed under the BSD 2-Clause License. See the LICENSE file in the project root for license information.
 *  See the NOTICE file in the project root for additional information regarding copyright ownership.
 */
package com.linkedin.datastream.common;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.reflect.Nullable;
import org.apache.commons.lang.Validate;
import org.apache.kafka.common.header.Headers;


/**
 * This type represents messages sent by Brooklin to destination systems in order to communicate data change events
 * in <a href="https://en.wikipedia.org/wiki/Change_data_capture">change data capture</a> scenarios.
 * It exposes properties that identify the exact record that was changed (e.g. primary key in a DB table), its new value,
 * and (optionally) previous value.
 */
public class BrooklinEnvelope {

  private Object _previousValue;

  private Object _key;

  private Object _value;

  private Map<String, String> _metadata;

  private Headers _headers;

  /**
   * Construct a BrooklinEnvelope using record key, value, and metadata
   * @param key The record key (e.g. primary key)
   * @param value The new record value
   * @param metadata Additional metadata to associate with the change event
   */
  public BrooklinEnvelope(Object key, Object value, Map<String, String> metadata) {
    this(key, value, null, metadata, null);
  }

  /**
   * Construct a BrooklinEnvelope using record key, value, and metadata
   * @param key The record key (e.g. primary key)
   * @param previousValue The old record value
   * @param value The new record value
   * @param metadata Additional metadata to associate with the change event
   * @param headers Kafka Headers
   */
  public BrooklinEnvelope(@Nullable Object key, @Nullable Object value, @Nullable Object previousValue,
      Map<String, String> metadata, Headers headers) {
    Validate.notNull(metadata, "metadata cannot be null");
    setKey(key);
    setValue(value);
    setPreviousValue(previousValue);
    setMetadata(metadata);
    setHeaders(headers);
  }

  /**
   * Construct a BrooklinEnvelope using record key, value, and metadata
   * @param key The record key (e.g. primary key)
   * @param previousValue The old record value
   * @param value The new record value
   * @param metadata Additional metadata to associate with the change event
   */
  public BrooklinEnvelope(@Nullable Object key, @Nullable Object value, @Nullable Object previousValue,
                          Map<String, String> metadata) {
    this(key, value, previousValue, metadata, null);
  }

  /**
   * Note: be careful about the return type of this API. It's Optional<Object>
   * instead of Object, which is inconsistent with the rest of the APIs in the
   * file.
   *
   * The return type here (Optional<Object>) is not a good idea. It's inconsistent
   * with other APIs in the class. Plus, "Optional" itself is an "Object". So it's extremely
   * error-prone.
   */
  public Optional<Object> getPreviousValue() {
    return Optional.ofNullable(_previousValue);
  }

  /**
   * Set the previous value of the record
   */
  public void setPreviousValue(Object previousValue) {
    _previousValue = previousValue instanceof Optional ? ((Optional<?>) previousValue).orElse(null) : previousValue;
  }

  @Nullable
  @Deprecated
  public Object getKey() {
    return _key;
  }

  /**
   * Set the record key
   */
  public void setKey(@Nullable Object key) {
    _key = key instanceof Optional ? ((Optional<?>) key).orElse(null) : key;
  }

  /**
   * Get the record key associated with this change event, if any
   */
  public Optional<Object> key() {
    return Optional.ofNullable(_key);
  }

  @Nullable
  @Deprecated
  public Object getValue() {
    return _value;
  }

  /**
   * Set the record value
   */
  public void setValue(@Nullable Object value) {
    _value = value instanceof Optional ? ((Optional<?>) value).orElse(null) : value;
  }

  /**
   * Get the record value associated with this change event, if any
   */
  public Optional<Object> value() {
    return Optional.ofNullable(_value);
  }

  /**
   * Get the metadata associated with this change event
   */
  public Map<String, String> getMetadata() {
    return _metadata;
  }

  /**
   * Set the metadata associated with this change event
   */
  public void setMetadata(Map<String, String> metadata) {
    _metadata = metadata;
  }


  /**
   * Get the kafka headers
   */
  public Headers getHeaders() {
    return _headers;
  }

  /**
   * Set the kafka headers
   */
  public void setHeaders(Headers headers) {
    _headers = headers;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    BrooklinEnvelope task = (BrooklinEnvelope) o;
    return Objects.equals(_previousValue, task._previousValue) && Objects.equals(_key, task._key) && Objects.equals(
        _value, task._value) && Objects.equals(_metadata, task._metadata) && Objects.equals(_headers, task._headers);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_key, _value, _previousValue, _metadata, _headers);
  }

  @Override
  public String toString() {
    return String.format("Key:(%s), Value:(%s), PreviousValue:(%s), Metadata=(%s), Headers=(%s)", _key, _value, _previousValue,
        _metadata, _headers);
  }
}
