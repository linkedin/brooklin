package com.linkedin.datastream.common;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.avro.reflect.Nullable;
import org.apache.commons.lang.Validate;


public class BrooklinEnvelope {

  private Object _previousValue;

  private Object _key;

  private Object _value;

  private Map<String, String> _metadata;

  public BrooklinEnvelope(Object key, Object value, Map<String, String> metadata) {
    this(key, value, null, metadata);
  }

  public BrooklinEnvelope(@Nullable Object key, @Nullable Object value, @Nullable Object previousValue,
      Map<String, String> metadata) {
    Validate.notNull(metadata, "metadata cannot be null");
    _key = key;
    _value = value;
    _previousValue = previousValue;
    _metadata = metadata;
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

  public void setPreviousValue(Object previousValue) {
    _previousValue = previousValue;
  }

  public void setKey(@Nullable Object key) {
    _key = key;
  }

  public void setValue(@Nullable Object value) {
    _value = value;
  }

  public void setMetadata(Map<String, String> metadata) {
    _metadata = metadata;
  }

  @Nullable
  @Deprecated
  public Object getKey() {
    return _key;
  }

  public Optional<Object> key() {
    return Optional.ofNullable(_key);
  }

  @Nullable
  @Deprecated
  public Object getValue() {
    return _value;
  }

  public Optional<Object> value() {
    return Optional.ofNullable(_value);
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
