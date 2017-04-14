package com.linkedin.datastream.common;

import java.util.Map;
import java.util.Objects;


public class BrooklinEnvelope {

  private Object _previousValue;

  private Object _key;

  private Object _value;

  private Map<String, String> _metadata;

  public BrooklinEnvelope(Object key, Object value, Object previousValue, Map<String, String> metadata) {
    _key = key;
    _value = value;
    _previousValue = previousValue;
    _metadata = metadata;
  }

  public Object getPreviousValue() {
    return _previousValue;
  }

  public void setPreviousValue(Object previousValue) {
    _previousValue = previousValue;
  }

  public void setKey(Object key) {
    _key = key;
  }

  public void setValue(Object value) {
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
