package com.linkedin.datastream.common;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.lang.Validate;


public class BrooklinEnvelope {

  private Optional<Object> _previousValue;

  private Object _key;

  private Object _value;

  private Map<String, String> _metadata;

  public BrooklinEnvelope(Object key, Object value, Map<String, String> metadata) {
    this(key, value, null, metadata);
  }

  public BrooklinEnvelope(Object key, Object value, Object previousValue, Map<String, String> metadata) {
    Validate.notNull(key, "key cannot be null");
    Validate.notNull(key, "value cannot be null");
    Validate.notNull(metadata, "metadata cannot be null");
    _key = key;
    _value = value;
    _previousValue = Optional.ofNullable(previousValue);
    _metadata = metadata;
  }

  public Optional<Object> getPreviousValue() {
    return _previousValue;
  }

  public void setPreviousValue(Object previousValue) {
    _previousValue = Optional.ofNullable(previousValue);
  }

  public void setKey(Object key) {
    Validate.notNull(key, "key cannot be null");
    _key = key;
  }

  public void setValue(Object value) {
    Validate.notNull(value, "value cannot be null");
    _value = value;
  }

  public void setMetadata(Map<String, String> metadata) {
    _metadata = metadata;
  }

  public Object getKey() {
    return _key;
  }

  public Object getValue() {
    return _value;
  }

  public Map<String, String> getMetadata() {
    return _metadata;
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
        _value, task._value) && Objects.equals(_metadata, task._metadata);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_key, _value, _previousValue, _metadata);
  }

  @Override
  public String toString() {
    return String.format("Key:(%s), Value:(%s), PreviousValue:(%s), Metadata=(%s)", _key, _value, _previousValue,
        _metadata);
  }
}
