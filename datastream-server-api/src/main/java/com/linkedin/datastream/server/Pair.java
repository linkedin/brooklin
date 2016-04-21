package com.linkedin.datastream.server;

public class Pair<K, V> {

  private final V _value;
  private final K _key;

  public Pair(K key,  V value) {
    _key = key;
    _value = value;
  }

  public V getValue() {
    return _value;
  }

  public K getKey() {
    return _key;
  }
}
