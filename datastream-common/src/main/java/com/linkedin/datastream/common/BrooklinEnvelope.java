package com.linkedin.datastream.common;

import java.util.Map;


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
}
